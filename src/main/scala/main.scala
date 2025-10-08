import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.myproject.{DataQuality, Transformation, Postgre, CDC}

object main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Application Log Processor")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
    implicit val implicitSpark = spark

    // Define schema for JSON logs
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

    // Read JSON logs
    val rawDF = spark.read
      .schema(logSchema)
      .option("multiline", "true")
      .json("/home/tamanna/Dataset/raw_data.json")

    // Reading Modified Data
    val modifiedDF = spark.read
      .schema(logSchema)
      .option("multiline", "true")
      .json("/home/tamanna/Dataset/modified_data.json")

    //  Call Data Quality Pipeline
    val cleanedDF = DataQuality.runDataQualityPipeline(rawDF)(spark)
    val cleanedModifiedDF = DataQuality.runDataQualityPipeline(modifiedDF)(spark)

    cleanedModifiedDF.write
      .format("delta")
      .mode("overwrite") // or "append" if you want to keep existing data
      .save("/home/tamanna/Dataset/Logs/delta_logs")


    //  Call Transformation Pipeline
    Transformation.runAllTransformations(cleanedDF)

    val badRecordsDF = DataQuality.getBadRecords(rawDF)(spark)
    val filledBadRecordsDF = DataQuality.getFilledBadRecords(badRecordsDF)

    // Call PostgreSQL Write Function
    Postgre.uploadCleanedData(cleanedDF)
    Postgre.uploadDirtyData(filledBadRecordsDF)

    // Call CDC Pipeline
    CDC.applyCDC(cleanedDF, cleanedModifiedDF, "/home/tamanna/Dataset/Logs/delta_logs")

    // Show final Delta table
    spark.read
      .format("delta")
      .load("/home/tamanna/Dataset/Logs/delta_logs")
      .show(false)

    spark.stop()
  }
}