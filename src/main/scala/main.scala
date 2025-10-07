import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Application Log Processor")
      .master("local[*]")
      .getOrCreate()

    val logSchema = StructType(Array(
      StructField("log_id", StringType, nullable = true),
      StructField("timestamp", StringType, nullable = true),
      StructField("user_id", StringType, nullable = true),
      StructField("action", StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("ip_address", StringType, nullable = true),
      StructField("region", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    ))

    val logDF = spark.read
      .schema(logSchema)
      .option("multiline", "true")
      .json("/home/runali/log_data.json")

    // Convert timestamp to proper type (optional)
    val logDFWithTime = logDF.withColumn(
      "timestamp",
      to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

    logDFWithTime.show(5, truncate = false)
    logDFWithTime.printSchema()

    // Hello
    spark.stop()
  }
}