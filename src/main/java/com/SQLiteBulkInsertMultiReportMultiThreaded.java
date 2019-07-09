package com;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.xml.bind.DatatypeConverter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sqlite.jdbc4.JDBC4Connection;

/**
 * This class shares the same connection among multiple threads of a single report.
 */
public class SQLiteBulkInsertMultiReportMultiThreaded {

  private static final Logger logger = LogManager.getLogger(SQLiteBulkInsertMultiReportMultiThreaded.class);

  final String URL_WITHOUT_FNAME = "jdbc:sqlite:file:E:/embedded-dbs/SQLite-DB-FILES/";


  private String expectedCheckSum;

  public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {


    SQLiteBulkInsertMultiReportMultiThreaded test = new SQLiteBulkInsertMultiReportMultiThreaded();

    test.startTest();

    logger.info("Exiting....");
  }


  private void startTest() throws SQLException, ExecutionException, InterruptedException {

    final String data = readFile();
    this.expectedCheckSum = getMD5Hash(data);

    logger.info("data.length={}, MD5CheckSum={}, data={}", data.length(), this.expectedCheckSum, data);

    long start = System.currentTimeMillis();

        String[] reportNames = {"report_1", "report_2", "report_3", "report_4", "report_5", "report_6", "report_7",
            "report_8"};

    //String[] reportNames = {"report_1"};

    int noOfThreads = 5;
    int noOfThreadPools = reportNames.length;

    logger.info("Test Started, noOfReports={}, noOfThreadPools={}, noOfThreads={}", reportNames.length, noOfThreadPools,
        noOfThreads);

    multipleReport(reportNames, noOfThreadPools, noOfThreads);
    createIndex(reportNames, noOfThreadPools);
    read(reportNames, noOfThreadPools);

    logger.info("Test Completed, Time={}", (System.currentTimeMillis() - start));
  }

  private void multipleReport(String[] reportNames, int noOfThreadPools, int noOfThreads) throws SQLException {

    long start = System.currentTimeMillis();

    final ExecutorService[] threadPools = getThreadPools(noOfThreadPools, noOfThreads);

    int ctr = 0;

    List<List<Future<Connection>>> listOfFutureLists = new ArrayList<>();

    for (String reportName : reportNames) {

      final String url = URL_WITHOUT_FNAME + reportName;

      final ExecutorService threadPool = threadPools[ctr++];

      try {

        createTable(url);

        //        final Properties properties = new Properties();
        //        properties.setProperty("journal_mode", "WAL");
        //
        //        Connection conn = DriverManager.getConnection(url,properties);

        Connection conn = DriverManager.getConnection(url);
        conn.setAutoCommit(false);
        final List<Future<Connection>> futures = bulkInsertReport(conn, threadPool, noOfThreads);
        listOfFutureLists.add(futures);

      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    for (List<Future<Connection>> futures : listOfFutureLists) {

      Connection conn = null;
      for (Future<Connection> future : futures) {
        try {
          conn = future.get();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      }
      conn.commit();

      final String url = ((JDBC4Connection) conn).getDatabase().getUrl();

      conn.close();

      logger.info("Write completed for Report={}, Time={}", url, (System.currentTimeMillis() - start));

    }

    logger.info("################## Bulk Insert for all Reports Completed, Time={}",
        (System.currentTimeMillis() - start));

  }

  private ExecutorService[] getThreadPools(int noOfThreadPools, int noOfThreads) {

    ExecutorService[] threadPools = new ExecutorService[noOfThreadPools];

    for (int ctr = 0; ctr < noOfThreadPools; ctr++) {
      threadPools[ctr] = Executors.newFixedThreadPool(noOfThreads);
    }

    return threadPools;
  }

  public List<Future<Connection>> bulkInsertReport(final Connection conn, final ExecutorService threadPool,
      final int noOfThreads) {

    long start = System.currentTimeMillis();

    int noOfRows = 1_000 / noOfThreads;
    final int batchSize = 1_00_000;

    final Callable<Connection> callable = new Callable<Connection>() {
      @Override
      public Connection call() {
        insertData(conn, noOfRows, batchSize);
        return conn;
      }
    };

    List<Future<Connection>> futures = new ArrayList<>();
    for (int ctr = 0; ctr < noOfThreads; ctr++) {
      futures.add(threadPool.submit(callable));
    }

    return futures;
  }

  //CREATE INDEX StudentNameIndex ON Students(StudentName);
  private void createIndex(String url) {

    long start = System.currentTimeMillis();
    try (Connection conn = DriverManager.getConnection(url);
        PreparedStatement pStmt = conn
            .prepareStatement("CREATE INDEX TEST_IDX_1 ON TEST_INSERT(IP, OS, PORT, TYPE, SEVERITY)")) {

      pStmt.execute();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    logger.info("Index created... URL={}, Time={}", url, (System.currentTimeMillis() - start));

  }

  private void insertData(Connection conn, final int noOfRows, final int batchSize) {

    //    logger.info("###################Insert Data started..... NoOfRecords={}, Thread={}", noOfRows,
    //        Thread.currentThread().getName());

    long start = System.currentTimeMillis();

    final String str = readFile();
    //System.out.println("str=" + str);

    int ip = 27000;
    String os = "Linux";
    int port = 12345;
    int severity = 5;
    String type = "VULN";
    String data = str;

    int ctr = 0;
    try (PreparedStatement pStmt = conn
        .prepareStatement("insert into TEST_INSERT (IP, OS, PORT, TYPE, SEVERITY, DATA) values(?,?,?,?,?,?)");) {

      for (ctr = 0; ctr < noOfRows; ctr++) {

        pStmt.setInt(1, ctr);
        pStmt.setString(2, os);
        pStmt.setInt(3, port);
        pStmt.setString(4, type);
        pStmt.setInt(5, severity);
        pStmt.setString(6, data);

        pStmt.addBatch();

        if (ctr % batchSize == 0) {
          pStmt.executeBatch();
        }
      }

      pStmt.executeBatch();

    } catch (SQLException e) {
      logger.error(e.getMessage(), e);
    }

    //    logger.info("Insert Time={}, NoOfRecords={}, Thread={}", (System.currentTimeMillis() - start), ctr,
    //        Thread.currentThread().getName());
  }

  private void createTable(String url) {

    try (Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();) {
      statement.executeUpdate(
          "create table TEST_INSERT (IP integer, OS string, PORT integer, TYPE string, SEVERITY integer, DATA string)");
    } catch (SQLException e) {
      logger.error(e.getMessage(), e);
    }
    logger.info("Table is created");
  }

  private String readFile() {

    File f = new File("src/main/resources/sample.csv");

    StringBuilder tmp = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new FileReader(f))) {
      String readLine = null;
      while ((readLine = br.readLine()) != null) {
        tmp.append(readLine);
      }
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
    return tmp.toString();
  }

  /**
   * Multithreaded index creation
   */
  private void createIndex(String[] reportNames, int noOfThreadPools) throws ExecutionException, InterruptedException {

    long start = System.currentTimeMillis();

    final ExecutorService[] threadPools = getThreadPools(noOfThreadPools, 1);

    int idx = 0;

    List<Future<?>> futures = new ArrayList<>();

    for (String reportName : reportNames) {

      final String url = URL_WITHOUT_FNAME + reportName;

      final Runnable runnable = new Runnable() {
        @Override
        public void run() {
          createIndex(url);
        }
      };

      final ExecutorService threadPool = threadPools[idx++];

      futures.add(threadPool.submit(runnable));
    }

    for (Future future : futures) {
      future.get();
    }

    logger.info("Index Created On total tables={}, Time={} ", reportNames.length, (System.currentTimeMillis() - start));
  }

  /**
   * Multithreaded Read
   */
  private void read(String[] reportNames, int noOfThreadPools) throws ExecutionException, InterruptedException {

    long start = System.currentTimeMillis();

    final ExecutorService[] threadPools = getThreadPools(noOfThreadPools, 1);

    int idx = 0;

    List<Future<?>> futures = new ArrayList<>();

    for (String reportName : reportNames) {

      final String url = URL_WITHOUT_FNAME + reportName;

      final Runnable runnable = new Runnable() {
        @Override
        public void run() {
          readData(url);
        }
      };

      final ExecutorService threadPool = threadPools[idx++];

      futures.add(threadPool.submit(runnable));
    }

    for (Future future : futures) {
      future.get();
    }

    logger.info("Total Reports read={}, Time={} ", reportNames.length, (System.currentTimeMillis() - start));
  }

  private void readData(String url) {

    int ctr = 0;
    long start = System.currentTimeMillis();

    try (Connection conn = DriverManager.getConnection(url);
        PreparedStatement pStmt = conn
            .prepareStatement("SELECT IP, OS, PORT, TYPE, SEVERITY, DATA from TEST_INSERT order by IP desc");
        ResultSet rs = pStmt.executeQuery()) {

      while (rs.next()) {

        final int ip = rs.getInt("IP");
        final String os = rs.getString("OS");
        final int port = rs.getInt("PORT");
        final String type = rs.getString("TYPE");
        final int severity = rs.getInt("SEVERITY");
        final String data = rs.getString("DATA");

        //System.out.println("IP="+ip);

        //        final String md5Hash = getMD5Hash(data);
        //
        //        if (!this.expectedCheckSum.equals(md5Hash)) {
        //
        //          logger.error("\n\nChecksum not matching, actualdata={}, url={}, ip={}, actual-checksum={}", data, url, ip,md5Hash);
        //
        //          throw new IllegalArgumentException("CheckSum not matching");
        //        }

        ctr++;
      }


    } catch (SQLException e) {
      e.printStackTrace();
    }

    logger.info(" URL={}, Read Time={}, NoOfRecords={}", url, (System.currentTimeMillis() - start), ctr);

  }


  // Java method to create MD5 checksum
  private String getMD5Hash(String data) {
    String result = null;
    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      byte[] hash = digest.digest(data.getBytes("UTF-8"));
      return bytesToHex(hash); // make it printable
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return result;
  }

  private String bytesToHex(byte[] hash) {
    return DatatypeConverter.printHexBinary(hash).toLowerCase();
  }
}
