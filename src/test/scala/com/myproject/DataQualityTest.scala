package com.myproject

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

case class LogRecord(
                      log_id: String,
                      timestamp: String,
                      user_id: String,
                      action: String,
                      status: String,
                      ip_address: String,
                      region: String,
                      version: String
                    )

class DataQualityTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("DataQualityTest")
    .master("local[*]")
    .getOrCreate()

  val sparkSession = spark
  import sparkSession.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  override def afterAll(): Unit = {
    spark.stop()
  }

  "checkNullUserId" should "return rows with null user_id" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq(("log1", null)).toDF("log_id", "user_id")
    val result = DataQuality.checkNullUserId(df)
    result.count() shouldBe 1
  }

  "checkNullStatus" should "return rows with null status" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq(("log1", null)).toDF("log_id", "status")
    val result = DataQuality.checkNullStatus(df)
    result.count() shouldBe 1
  }

  "checkInvalidStatus" should "return rows with status not success or failed" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq(("log1", "unknown")).toDF("log_id", "status")
    val result = DataQuality.checkInvalidStatus(df)
    result.count() shouldBe 1
  }

  "checkInvalidTimestamp" should "return rows with unparsable timestamps" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq(("log1", "invalid")).toDF("log_id", "timestamp")
    val result = DataQuality.checkInvalidTimestamp(df)
    result.count() shouldBe 1
  }

  "checkMissingIp" should "return rows with null or empty IP address" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq(("log1", null), ("log2", "")).toDF("log_id", "ip_address")
    val result = DataQuality.checkMissingIp(df)
    result.count() shouldBe 2
  }

  "checkInvalidRegion" should "return rows with region not in allowed list" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq(("log1", "XX")).toDF("log_id", "region")
    val result = DataQuality.checkInvalidRegion(df)
    result.count() shouldBe 1
  }

  "checkDuplicateLogId" should "return rows with duplicate log_id" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq(("log1", "duplicate"), ("log1", "duplicate")).toDF("log_id", "note")
    val result = DataQuality.checkDuplicateLogId(df)
    result.count() shouldBe 2
  }

  "checkEmptyAction" should "return rows with null or empty action" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq(("log1", null), ("log2", "")).toDF("log_id", "action")
    val result = DataQuality.checkEmptyAction(df)
    result.count() shouldBe 2
  }

  "checkInvalidVersion" should "return rows with version not matching vX.X" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq(("log1", "vX")).toDF("log_id", "version")
    val result = DataQuality.checkInvalidVersion(df)
    result.count() shouldBe 1
  }

  "checkFutureTimestamp" should "return rows with timestamp in the future" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val futureTs = java.time.ZonedDateTime.now().plusHours(1).format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
    val df = Seq(("log1", futureTs)).toDF("log_id", "timestamp")
    val result = DataQuality.checkFutureTimestamp(df)
    result.count() shouldBe 1
  }

  "getFilledBadRecords" should "fill nulls with default values" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val df = Seq(
      LogRecord("logX", null, null, null, null, null, null, null)
    ).toDF()

    val filled = DataQuality.getFilledBadRecords(df)
    val row = filled.first()

    row.getAs[String]("user_id") shouldBe "UNKNOWN_USER"
    row.getAs[String]("status") shouldBe "unknown"
    row.getAs[String]("ip_address") shouldBe "0.0.0.0"
    row.getAs[String]("action") shouldBe "unknown"
    row.getAs[String]("region") shouldBe "UNKNOWN"
    row.getAs[String]("version") shouldBe "v0.0"
  }

  "runDataQualityPipeline" should "remove bad records and return cleaned data" in {

  implicit val implicitSpark: SparkSession = spark

    val rawJson = Seq(
      """{"log_id": "log1", "timestamp": "2025-10-18T08:00:00Z", "user_id": "user_123", "action": "login", "status": "success", "ip_address": "192.168.1.1", "region": "IN", "version": "v1.0"}""",
      """{"log_id": "log2", "timestamp": "2025-10-18T08:01:00Z", "user_id": null, "action": "logout", "status": "success", "ip_address": "192.168.1.2", "region": "US", "version": "v1.0"}""",
      """{"log_id": "log3", "timestamp": "invalid", "user_id": "user_456", "action": "click", "status": "fail", "ip_address": "192.168.1.3", "region": "EU", "version": "v1.0"}""",
      """{"log_id": "log4", "timestamp": "2025-10-18T08:03:00Z", "user_id": "user_789", "action": "", "status": "success", "ip_address": "", "region": "XX", "version": "v1.0"}""",
      """{"log_id": "log1", "timestamp": "2025-10-18T08:04:00Z", "user_id": "user_101", "action": "login", "status": "unknown", "ip_address": "192.168.1.5", "region": "IN", "version": "vX"}"""
    ).toDS()

    val testDF = spark.read.json(rawJson)
    val cleanedDF = DataQuality.runDataQualityPipeline(testDF)

    val logIds = cleanedDF.select("log_id").as[String].collect()
    logIds shouldBe empty

    cleanedDF.columns should contain allOf(
      "log_id", "timestamp", "user_id", "action", "status", "ip_address", "region", "version"
    )

    val parsedTimestamps = cleanedDF
      .select(date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
      .as[String]
      .collect()

    parsedTimestamps.foreach { ts =>
      ts should fullyMatch regex """\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"""
    }
  }
}