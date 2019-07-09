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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Insert Time=59009, NoOfRecords=10000000
 * Index Time=25464
 * Read Time=96107, NoOfRecords=10000000
 */
public class BulkInsert {

  private static final Logger logger = LogManager.getLogger(BulkInsert.class);


  public static void main(String[] args) {

    String dbFileName = "1aa36d72-43ad-4146-821b-1912069bd5b5.db";
    final String url = "jdbc:sqlite:" + dbFileName;

    BulkInsert test = new BulkInsert();

    test.createTable(url);
    test.insertData(url);
    test.createIndex(url);
    test.readData(url);
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

  private void insertData(String url) {

    long start = System.currentTimeMillis();

    final String str = readFile();
    System.out.println("str=" + str);

    int ip = 27000;
    String os = "Linux";
    int port = 12345;
    int severity = 5;
    String type = "VULN";
    String data = str;

    int noOfRows = 10_000_000;
    final int batchSize = 1_00_000;
    int ctr = 0;

    try (Connection conn = DriverManager.getConnection(url);
        PreparedStatement pStmt = conn
            .prepareStatement("insert into TEST_INSERT (IP, OS, PORT, TYPE, SEVERITY, DATA) values(?,?,?,?,?,?)");) {

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
          pStmt.executeBatch();
        }
      }

      pStmt.executeBatch();

      conn.commit();
    } catch (SQLException e) {
      logger.error(e.getMessage(), e);
    }

    logger.info("Insert Time={}, NoOfRecords={}", (System.currentTimeMillis() - start), ctr);
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
