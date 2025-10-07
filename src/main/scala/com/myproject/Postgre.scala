package com.myproject
import org.apache.spark.sql.DataFrame
import java.util.Properties

object Postgre {

  val jdbcUrl = "jdbc:postgresql://localhost:5432/mydb"
  val props = new Properties()
  props.setProperty("user", "mahek")
  props.setProperty("password", "mahek")
  props.setProperty("driver", "org.postgresql.Driver")

  def uploadCleanedData(df: DataFrame): Unit = {
    df.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "clean_logs", props)
    println("Cleaned data uploaded to PostgreSQL.")
  }

  def uploadDirtyData(df: DataFrame): Unit = {
    df.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "dirty_logs", props)
    println("Dirty data uploaded to PostgreSQL.")
  }
}