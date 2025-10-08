package com.myproject

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataQuality {

  def checkNullUserId(df: DataFrame): DataFrame = {
    println("1️. Check for null user_id:")
    val result = df.filter(col("user_id").isNull)
    result.show(false)
    result
  }

  def checkNullStatus(df: DataFrame): DataFrame = {
    println("2️. Check for null status:")
    val result = df.filter(col("status").isNull)
    result.show(false)
    result
  }

  def checkInvalidStatus(df: DataFrame): DataFrame = {
    println("3️. Check for invalid status:")
    val result = df.filter(!col("status").isin("success", "failed"))
    result.show(false)
    result
  }

  def checkInvalidTimestamp(df: DataFrame): DataFrame = {
    println("4️. Check for invalid timestamp:")
    val result = df.filter(to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").isNull)
    result.show(false)
    result
  }

  def checkMissingIp(df: DataFrame): DataFrame = {
    println("5️. Check for missing IP address:")
    val result = df.filter(col("ip_address").isNull || length(trim(col("ip_address"))) === 0)
    result.show(false)
    result
  }

  def checkInvalidRegion(df: DataFrame): DataFrame = {
    println("6️. Check for invalid region:")
    val result = df.filter(!col("region").isin("IN", "US", "EU", "AU"))
    result.show(false)
    result
  }

  def checkDuplicateLogId(df: DataFrame): DataFrame = {
    println("7️. Check for duplicate log_id:")
    val dupIds = df.groupBy("log_id").count().filter("count > 1").select("log_id")
    val result = df.join(dupIds, Seq("log_id"), "inner")
    result.show(false)
    result
  }

  def checkEmptyAction(df: DataFrame): DataFrame = {
    println("8️. Check for empty or null action:")
    val result = df.filter(col("action").isNull || length(trim(col("action"))) === 0)
    result.show(false)
    result
  }

  def checkInvalidVersion(df: DataFrame): DataFrame = {
    println("9️. Check for invalid version format:")
    val result = df.filter(!col("version").rlike("^v\\d+\\.\\d+$"))
    result.show(false)
    result
  }

  def checkFutureTimestamp(df: DataFrame): DataFrame = {
    println("10 Check for future timestamps:")
    val result = df.filter(to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'") > current_timestamp())
    result.show(false)
    result
  }

  def getBadRecords(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    checkNullUserId(df)
      .union(checkNullStatus(df))
      .union(checkInvalidStatus(df))
      .union(checkInvalidTimestamp(df))
      .union(checkMissingIp(df))
      .union(checkInvalidRegion(df))
      .union(checkDuplicateLogId(df))
      .union(checkEmptyAction(df))
      .union(checkInvalidVersion(df))
      .union(checkFutureTimestamp(df))
  }

  def getCleanedData(df: DataFrame, badRecords: DataFrame): DataFrame = df.except(badRecords)

  def getFilledBadRecords(badRecords: DataFrame): DataFrame = {
    val filledBadRecords = badRecords.na.fill(Map(
      "user_id" -> "UNKNOWN_USER",
      "status" -> "unknown",
      "ip_address" -> "0.0.0.0",
      "action" -> "unknown",
      "region" -> "UNKNOWN",
      "version" -> "v0.0"
    ))
    println("11. Filled Bad Records with default values:")
    filledBadRecords.show(false)
    filledBadRecords
  }

  def dropRecordsWithNullUserId(df: DataFrame): DataFrame = {
    val dropped = df.filter(col("user_id").isNotNull)
    println("12. Records after dropping rows with null user_id:")
    dropped.show(false)
    dropped
  }

  def runDataQualityPipeline(rawDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val logDF = rawDF.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    val rawCount = logDF.count()
    println(s"Raw Data: $rawCount records")
    logDF.show(false)

    val badRecordsDF = getBadRecords(logDF)
    val cleanedDF = getCleanedData(logDF, badRecordsDF)
    val filledBadRecordsDF = getFilledBadRecords(badRecordsDF)
    val droppedUserIdDF = dropRecordsWithNullUserId(logDF)

    val droppedUserIdCount = droppedUserIdDF.count()
    println(s"Records after dropping null user_id: $droppedUserIdCount")

    val filledBadCount = filledBadRecordsDF.count()
    println(s"Filled Bad Records (with default values): $filledBadCount records")

    val cleanedCount = cleanedDF.count()
    println(s"Cleaned Data: $cleanedCount records")

    val badCount = badRecordsDF.count()
    println(s"Bad Records: $badCount records")

    // Handle skewness with salting
    val saltedDF = cleanedDF.withColumn("salt", (rand() * 10).cast("int"))
    val skewHandledDF = saltedDF.repartition(col("user_id"), col("salt"))
    println("Final Skew-Handled Data Sample:")
    skewHandledDF.show(70, truncate = false)

    // Write cleaned data with partitioning and simulated bucketing
    cleanedDF
      .repartition(col("user_id"))
      .sortWithinPartitions("user_id")
      .write
      .partitionBy("region")
      .mode("overwrite")
      .parquet("/home/shravani/Dataset/Logs/partitioned_bucketed_logs")

    println("Cleaned data written to partitioned Parquet.")

    // Return cleanedDF for transformation
    cleanedDF
  }
}