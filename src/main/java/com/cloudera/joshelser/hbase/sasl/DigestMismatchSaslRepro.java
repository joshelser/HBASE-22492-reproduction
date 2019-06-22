package com.cloudera.joshelser.hbase.sasl;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class DigestMismatchSaslRepro implements Runnable {
  public static class PositiveNonZero implements IParameterValidator {

    public void validate(String name, String value)
        throws ParameterException {
      int n = Integer.parseInt(value);
      if (n < 1) {
        throw new ParameterException("Parameter " + name
            + " should be greater than 0 (found " + value +")");
      }
    }

  }
  public static class Opts {
    @Parameter(names= {"-t", "--threads"}, description="Number of threads", validateWith= {PositiveNonZero.class})
    private int numThreads = 1;
    @Parameter(names= {"-n", "--name"}, description="HBase table name")
    private String tableName;
    @Parameter(names= {"-h", "--help"}, help = true)
    private boolean help;
    @Parameter(names= {"-s", "--splits"}, description = "Number of split points", validateWith = {PositiveNonZero.class})
    private int numSplits = 0;
    @Parameter(names= {"-i"}, description = "Number of rows per thread", validateWith= {PositiveNonZero.class})
    private int numRows = 0;
    @Parameter(names= {"-p", "--principal"}, description="Kerberos principal to use to obtain delegation token")
    private String principal;
    @Parameter(names= {"-k", "--keytab"}, description="Kerberos keytab for principal")
    private String keytab;
  }

  private final Configuration conf;
  private final Opts opts;
  private final TableName tableName;

  private static final Logger LOG = LoggerFactory.getLogger(DigestMismatchSaslRepro.class);
  private static final int ROWKEY_LENGTH = 20;
  private static final int CELL_LENGTH = 80;
  private static final byte[] FAM = Bytes.toBytes("value");
  private static final byte[] QUAL1 = Bytes.toBytes("col1");
  private static final byte[] QUAL2 = Bytes.toBytes("col2");

  public DigestMismatchSaslRepro(Configuration conf, Opts opts) {
    this.conf = conf;
    this.opts = opts;
    this.tableName = TableName.valueOf(opts.tableName);
  }

  @Override public void run() {
    LOG.info("Ensuring fresh table exists");
    try {
      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(opts.principal, opts.keytab);
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override public Void run() throws Exception {
          ensureTableCreated();
          return null;
        }
      });
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
    
    ExecutorService svc = Executors.newFixedThreadPool(opts.numThreads);
    List<Future<Boolean>> futures = new ArrayList<>();
    for (int i = 0; i < opts.numThreads; i++) {
      futures.add(svc.submit(newPartition(i)));
    }
    for (Future<Boolean> future : futures) {
      try {
        if (!future.get()) {
          LOG.error("Saw failed task!");
        } else {
          LOG.info("Task completed");
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Failed to see task completion", e);
      }
    }
    svc.shutdown();
    try {
      svc.awaitTermination(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("ThreadPool failed to terminate", e);
    }
  }

  void ensureTableCreated() throws Exception{
    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      Admin admin = conn.getAdmin();
      if (admin.tableExists(tableName)) {
        if (admin.isTableEnabled(tableName)) {
          admin.disableTable(tableName);
        }
        admin.deleteTable(tableName);
      }
  
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor(FAM));
  
      byte[][] splits = new byte[opts.numSplits][];
      for (int i = 0; i < opts.numSplits; i++) {
        char splitPoint = (char) ((int)'a' + i);
        splits[i] = Bytes.toBytes(Character.toString(splitPoint));
      }
      admin.createTable(htd, splits);
      // Rough wag to let regions get assigned
      Thread.sleep(5000);
    }
  }

  Callable<Boolean> newPartition(int seed) {
    return new Callable<Boolean>() {
      @Override public Boolean call() throws Exception {
        // Get a delegation token
        Token<AuthenticationTokenIdentifier> token = getDelegationToken();
        LOG.info("Got delegation token");

        // Wrap a UGI around it
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(opts.principal);
        if (!ugi.addToken(token)) {
          throw new RuntimeException("Failed to set delegation token on UGI");
        }

        LOG.info("Starting to write data");
        // Write the data to hbase using that delegation token
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override public Void run() throws Exception {
            long start = System.currentTimeMillis();
            writeData(seed);
            long durationInMillis = System.currentTimeMillis() - start;
            LOG.info("RowsWritten: {}, Duration: {} ms", opts.numRows, durationInMillis);
            return null;
          }
        });

        return true;
      }
    };
  }

  void writeData(int seed) throws IOException {
    BufferedMutatorParams params = new BufferedMutatorParams(tableName);
    try (Connection conn = ConnectionFactory.createConnection(conf);
            BufferedMutator bm = conn.getBufferedMutator(params)) {
      final Random r = new Random(seed);
      byte[] rowkey = new byte[ROWKEY_LENGTH];
      byte[] value1 = new byte[CELL_LENGTH];
      byte[] value2 = new byte[CELL_LENGTH];
      for (int i = 0; i < opts.numRows; i++) {
        // Generate a leading character (a salt)
        char leadingChar = (char) ((int)'a' + r.nextInt(opts.numSplits));

        // Fill the rowkey with random data, but then put our salt at the head
        // of the rowkey.
        byte[] prefix = Bytes.toBytes(Character.toString(leadingChar));
        r.nextBytes(rowkey);
        System.arraycopy(prefix, 0, rowkey, 0, prefix.length);

        // Create the Put
        Put p = new Put(rowkey);
        r.nextBytes(value1);
        p.addColumn(FAM, QUAL1, value1);
        r.nextBytes(value2);
        p.addColumn(FAM, QUAL2, value2);

        // Add it to the BufferedMutator
        bm.mutate(p);

        if (i % 100000 == 0) {
          LOG.info("Wrote {} rows", i);
        }
      }
    }
  }

  Token<AuthenticationTokenIdentifier> getDelegationToken() {
    try {
      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(opts.principal, opts.keytab);
      return ugi.doAs(new PrivilegedExceptionAction<Token<AuthenticationTokenIdentifier>>() {
        @Override public Token<AuthenticationTokenIdentifier> run() throws Exception {
          try (Connection conn = ConnectionFactory.createConnection(conf)) {
            return TokenUtil.obtainToken(conn);
          }
        }
      });
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    // Force JUL to write to SLF4J so we get consistent logging
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();

    // Parse args
    final Opts opts = new Opts();
    JCommander jcmd = JCommander.newBuilder()
        .addObject(opts)
        .build();

    jcmd.parse(args);
    if (opts.help) {
      jcmd.usage();
      System.exit(1);
      return;
    }

    Configuration conf = HBaseConfiguration.create();
    DigestMismatchSaslRepro repro = new DigestMismatchSaslRepro(conf, opts);
    repro.run();
  }
}
