package com.myproject

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

object Transformation {

  def filterByRegionAndStatus(df: DataFrame, region: String, status: String): DataFrame = {
    df.filter(col("region") === region && col("status") === status)
  }

  def countByAction(df: DataFrame): DataFrame = {
    df.groupBy("action").count().orderBy(desc("count"))
  }

  def dailyActionSummary(df: DataFrame): DataFrame = {
    df.withColumn("date", to_date(col("timestamp")))
      .groupBy("date", "action")
      .agg(count("*").alias("action_count"))
      .orderBy("date", "action")
  }

  def topActiveUsers(df: DataFrame): DataFrame = {
    df.groupBy("user_id")
      .agg(count("*").alias("activity_count"))
      .orderBy(desc("activity_count"))
  }

  def versionUsageStats(df: DataFrame): DataFrame = {
    df.groupBy("version").count().orderBy(desc("count"))
  }

  def regionWiseSuccessRate(df: DataFrame): DataFrame = {
    df.groupBy("region", "status")
      .agg(count("*").alias("count"))
      .groupBy("region")
      .pivot("status")
      .agg(first("count"))
      .na.fill(0)
  }

  def runAllTransformations(df: DataFrame): Unit = {
    println("Filtered Records where region is India and status is Succes:")
    filterByRegionAndStatus(df, "IN", "success").show()

    println("Action Counts:")
    countByAction(df).show()

    println("Daily Action Summary:")
    dailyActionSummary(df).show()

    println("Top Active Users:")
    topActiveUsers(df).show()

    println("Version Usage Stats:")
    versionUsageStats(df).show()

    println("Region-wise Success Rate:")
    regionWiseSuccessRate(df).show()
  }

}