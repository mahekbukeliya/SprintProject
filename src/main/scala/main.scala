import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.myproject.{DataQuality}

object main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Application Log Processor")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

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
      .json("/home/mahek/Dataset/raw_data.json")

    //  Call Data Quality Pipeline
    val cleanedDF = DataQuality.runDataQualityPipeline(rawDF)(spark)

    //  Call Transformation Pipeline
//    Transformation.runAllTransformations(cleanedDF)

    spark.stop()
  }
}