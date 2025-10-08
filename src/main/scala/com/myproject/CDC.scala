package com.myproject

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import io.delta.tables._

object CDC {

  def applyCDC(rawDF: DataFrame, modifiedDF: DataFrame, deltaPath: String)(implicit spark: SparkSession): Unit = {
    val rawWithTS = rawDF.withColumn("event_time", current_timestamp())
      .withColumn("cdc_flag", lit("updated"))

    val modifiedWithTS = modifiedDF.withColumn("event_time", current_timestamp())
      .withColumn("cdc_flag", lit("inserted"))

    modifiedWithTS.write
      .format("delta")
      .option("mergeSchema", "true")
      .mode("overwrite")
      .save(deltaPath)

    val deltaTable = DeltaTable.forPath(spark, deltaPath)

    deltaTable.as("target")
      .merge(
        rawWithTS.as("source"),
        "target.log_id = source.log_id"
      )
      .whenMatched()
      .updateExpr(Map(
        "timestamp" -> "source.timestamp",
        "user_id" -> "source.user_id",
        "action" -> "source.action",
        "status" -> "source.status",
        "ip_address" -> "source.ip_address",
        "region" -> "source.region",
        "version" -> "source.version",
        "event_time" -> "source.event_time",
        "cdc_flag" -> "source.cdc_flag"
      ))
      .whenNotMatched()
      .insertExpr(Map(
        "log_id" -> "source.log_id",
        "timestamp" -> "source.timestamp",
        "user_id" -> "source.user_id",
        "action" -> "source.action",
        "status" -> "source.status",
        "ip_address" -> "source.ip_address",
        "region" -> "source.region",
        "version" -> "source.version",
        "event_time" -> "source.event_time",
        "cdc_flag" -> "source.cdc_flag"
      ))
      .execute()
  }
}
