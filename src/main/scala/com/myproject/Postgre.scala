package com.myproject
import org.apache.spark.sql.DataFrame
import java.util.Properties

object Postgre {

  val jdbcUrl = "jdbc:postgresql://localhost:5432/mydb"
  val props = new Properties()
  props.setProperty("user", "shravani")
  props.setProperty("password", "root")
  props.setProperty("driver", "org.postgresql.Driver")

  def uploadCleanedData(df: DataFrame): Unit = {
    val conn = java.sql.DriverManager.getConnection(jdbcUrl, "shravani", "root")
    val stmt = conn.createStatement()
    stmt.execute("TRUNCATE TABLE clean_logs")
    conn.close()

    df.write
      .mode("append")
      .jdbc(jdbcUrl, "clean_logs", props)
    println("Cleaned data uploaded to PostgreSQL.")
  }

  def uploadDirtyData(df: DataFrame): Unit = {
    val conn = java.sql.DriverManager.getConnection(jdbcUrl, "shravani", "root")
    val stmt = conn.createStatement()
    stmt.execute("TRUNCATE TABLE dirty_logs")
    conn.close()

    df.write
      .mode("append")
      .jdbc(jdbcUrl, "dirty_logs", props)
    println("Dirty data uploaded to PostgreSQL.")
  }

  def createAllViews(): Unit = {
    val conn = java.sql.DriverManager.getConnection(jdbcUrl, "shravani", "root")
    val stmt = conn.createStatement()

    // Create or update login_view
    stmt.execute(
      """
        |CREATE OR REPLACE VIEW login_view AS
        |SELECT * FROM clean_logs WHERE action = 'login'
        |""".stripMargin)

    // Create or update region_in_view
    stmt.execute(
      """
        |CREATE OR REPLACE VIEW region_in_view AS
        |SELECT * FROM clean_logs WHERE region = 'IN'
        |""".stripMargin)

    // Create or update failed_status_view
    stmt.execute(
      """
        |CREATE OR REPLACE VIEW failed_status_view AS
        |SELECT * FROM clean_logs WHERE status = 'failed'
        |""".stripMargin)

    conn.close()
    println("All views created or updated in PostgreSQL.")
  }
  def createStoredProcedure(): String = {
    val conn = java.sql.DriverManager.getConnection(jdbcUrl, "shravani", "root")
    val stmt = conn.createStatement()

    stmt.execute(
      """
        |CREATE OR REPLACE PROCEDURE log_summary()
        |LANGUAGE plpgsql
        |AS $$
        |BEGIN
        |  INSERT INTO log_summary_table (total_logs, failed_logs)
        |  SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'failed')
        |  FROM clean_logs;
        |END;
        |$$;
        |""".stripMargin)

    conn.close()
    "Stored procedure 'log_summary' created in PostgreSQL."
  }
}