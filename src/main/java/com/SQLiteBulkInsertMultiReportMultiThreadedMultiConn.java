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
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.bind.DatatypeConverter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class creates connection per thread, does not share the same connection.
 */
public class SQLiteBulkInsertMultiReportMultiThreadedMultiConn {

  private static final Logger logger = LogManager.getLogger(SQLiteBulkInsertMultiReportMultiThreadedMultiConn.class);

  final String URL_WITHOUT_FNAME = "jdbc:sqlite:file:E:/embedded-dbs/SQLite-DB-FILES/{0}";


  private String expectedCheckSum;


  public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {

    SQLiteBulkInsertMultiReportMultiThreadedMultiConn test = new SQLiteBulkInsertMultiReportMultiThreadedMultiConn();

    test.startTest();

    logger.info("Exiting....");
  }

  // Java method to create MD5 checksum
  private static String getMD5Hash(String data) {
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

  private static String bytesToHex(byte[] hash) {
    return DatatypeConverter.printHexBinary(hash).toLowerCase();
  }

  private void startTest() throws SQLException, ExecutionException, InterruptedException {

    final String data = readFile();
    this.expectedCheckSum = getMD5Hash(data);

    logger.info("data.length={}, MD5CheckSum={}, data={}", data.length(), this.expectedCheckSum, data);

    long start = System.currentTimeMillis();

    String[] reportNames = {"report_1", "report_2", "report_3", "report_4", "report_5", "report_6", "report_7",
        "report_8"};

    //    String[] reportNames = {"report_1"};

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

    List<List<Future<Boolean>>> listOfFutureLists = new ArrayList<>();

    for (String reportName : reportNames) {

      final String url = MessageFormat.format(URL_WITHOUT_FNAME, reportName);

      final ExecutorService threadPool = threadPools[ctr++];

      createTable(url);

      final Lock lockPerReport = new ReentrantLock();

      final List<Future<Boolean>> futures = bulkInsertReport(lockPerReport, url, threadPool, noOfThreads);
      listOfFutureLists.add(futures);

    }

    for (List<Future<Boolean>> futures : listOfFutureLists) {

      for (Future<Boolean> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      }

      logger.info("Write completed for Report, Time={}", (System.currentTimeMillis() - start));

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

  public List<Future<Boolean>> bulkInsertReport(final Lock lockPerReport, final String url,
      final ExecutorService threadPool, final int noOfThreads) {

    long start = System.currentTimeMillis();

    int noOfRows = 1_000_000 / noOfThreads;
    final int batchSize = 1_00_000;

    final Callable<Boolean> callable = new Callable<Boolean>() {
      @Override
      public Boolean call() {
        insertData(lockPerReport, url, noOfRows, batchSize);
        return true;
      }
    };

    List<Future<Boolean>> futures = new ArrayList<>();
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

  private void insertData(final Lock lockPerReport, String url, final int noOfRows, final int batchSize) {

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
    try (Connection conn = DriverManager.getConnection(url);
        PreparedStatement synchOffPstmt = conn.prepareStatement("PRAGMA synchronous=OFF");
        PreparedStatement pStmt = conn
            .prepareStatement("insert into TEST_INSERT (IP, OS, PORT, TYPE, SEVERITY, DATA) values(?,?,?,?,?,?)");) {

      synchOffPstmt.execute();

      conn.setAutoCommit(false);

      for (ctr = 0; ctr < noOfRows; ctr++) {

        pStmt.setInt(1, ctr);
        pStmt.setString(2, os);
        pStmt.setInt(3, port);
        pStmt.setString(4, type);
        pStmt.setInt(5, severity);
        pStmt.setString(6, data);

        pStmt.addBatch();

        if (ctr % batchSize == 0) {
          lockPerReport.lock();
          pStmt.executeBatch();
          conn.commit();

          lockPerReport.unlock();
        }
      }

      lockPerReport.lock();
      pStmt.executeBatch();
      conn.commit();
      lockPerReport.unlock();


    } catch (SQLException e) {
      logger.error(e.getMessage(), e);
    }

    //    logger.info("URL={}, Insert Time={}, NoOfRecords={}, Thread={}", url, (System.currentTimeMillis() - start), ctr,
    //        Thread.currentThread().getName());
  }

  private void createTable(String url) {

    try (Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();) {
      statement.executeUpdate(
          "create table TEST_INSERT (IP int, OS varchar, PORT int, TYPE varchar, SEVERITY int, DATA varchar)");
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

      final String url = MessageFormat.format(URL_WITHOUT_FNAME, reportName);

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

      final String url = MessageFormat.format(URL_WITHOUT_FNAME, reportName);
      ;

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

        final String md5Hash = getMD5Hash(data);

        if (!this.expectedCheckSum.equals(md5Hash)) {
          throw new IllegalArgumentException("CheckSum not matching, actual data=" + data + "\nurl=" + url);
        }

        //System.out.println("IP="+ip);
        ctr++;
      }


    } catch (SQLException e) {
      e.printStackTrace();
    }

    logger.info(" URL={}, Read Time={}, NoOfRecords={}", url, (System.currentTimeMillis() - start), ctr);

  }

}
