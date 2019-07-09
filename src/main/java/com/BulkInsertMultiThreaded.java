package com;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * No Of Threads=1, Insert Time=59009, NoOfRecords=10000000
 * Index Time=25464
 * Read Time=96107, NoOfRecords=10000000
 *
 * No of Threads=5,  Insert Completed Time=63046 ,  Insert Time=59009, NoOfRecords=10000000
 * Index Time=28987
 */
public class BulkInsertMultiThreaded {

  private static final Logger logger = LogManager.getLogger(BulkInsertMultiThreaded.class);


  public static void main(String[] args) {

    String dbFileName = "report_1.db";
    final String url = "jdbc:sqlite:" + dbFileName;

    BulkInsertMultiThreaded test = new BulkInsertMultiThreaded();

    test.createTable(url);
    test.bulkInsert(url);
    test.createIndex(url);

  }
  //
  //  private void multipleReport() {
  //
  //    long start = System.currentTimeMillis();
  //
  //    int noOfRows = 2_000_000;
  //    final int batchSize = 1_00_000;
  //    int noOfThreads = 5;
  //
  //    String[] reportNames = {"report_1", "report_2", "report_3", "report_4", "report_5", "report_6", "report_7",
  //        "report_8"};
  //
  //    for (String reportName : reportNames) {
  //
  //      final String url = "jdbc:sqlite:" + reportName;
  //
  //      try (Connection conn = DriverManager.getConnection(url)) {
  //
  //        final List<Thread> threads = getThreads(conn, url, noOfThreads, noOfRows, batchSize);
  //
  //        createTable(url);
  //        bulkInsert(url);
  //        createIndex(url);
  //
  //      } catch (SQLException e) {
  //        e.printStackTrace();
  //      }
  //
  //      logger.info("################## Bulk Insert Completed, Time={}", (System.currentTimeMillis() - start));
  //    }
  //  }

  public void bulkInsert(String url) {

    long start = System.currentTimeMillis();

    int noOfRows = 2_000_000;
    final int batchSize = 1_00_000;
    int noOfThreads = 5;

    //Connection conn = null;
    try (Connection conn = DriverManager.getConnection(url)) {

      conn.setAutoCommit(false);

      final List<Thread> threads = getThreads(conn, url, noOfThreads, noOfRows, batchSize);
      for (Thread thread : threads) {
        thread.start();
        logger.info("Thread={} started", thread.getName());
      }

      for (Thread thread : threads) {
        try {
          thread.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        logger.info("Thread={} is completed.", thread.getName());
      }
      conn.commit();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    logger.info("####################Insert Completed Time={}, url={} ", (System.currentTimeMillis() - start), url);
  }

  private List<Thread> getThreads(Connection conn, String url, int noOfThreads, int noOfRows, int batchSize) {

    List<Thread> threads = new ArrayList<>(noOfThreads);

    for (int ctr = 0; ctr < noOfThreads; ctr++) {

      final Runnable runnable = new Runnable() {
        @Override
        public void run() {
          insertData(url, conn, noOfRows, batchSize);
        }
      };

      threads.add(new Thread(runnable));

    }
    return threads;
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
    System.out.println("Index Time=" + (System.currentTimeMillis() - start));
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
        ctr++;
      }


    } catch (SQLException e) {
      e.printStackTrace();
    }

    logger.info("Read Time={}, NoOfRecords={}", (System.currentTimeMillis() - start), ctr);

  }

  private void insertData(String url, Connection conn, final int noOfRows, final int batchSize) {

    logger.info("###################Insert Data started..... NoOfRecords={}, Thread={}", noOfRows,
        Thread.currentThread().getName());

    long start = System.currentTimeMillis();

    final String str = readFile();
    System.out.println("str=" + str);

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

    logger.info("Insert Time={}, NoOfRecords={}, Thread={}", (System.currentTimeMillis() - start), ctr,
        Thread.currentThread().getName());
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
    String readLine = null;
    try (BufferedReader b = new BufferedReader(new FileReader(f))) {
      readLine = b.readLine();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
    return readLine;
  }
}
