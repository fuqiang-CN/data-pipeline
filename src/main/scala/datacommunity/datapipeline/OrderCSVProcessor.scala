package datacommunity.datapipeline

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object OrderCSVProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Order CSV Processor")
      .config("spark.driver.host", "127.0.0.1")
      .master("local")
      .getOrCreate()

    val inputPath = args(0)
    val outputPath = args(1)

    run(spark, inputPath, outputPath)

    spark.stop()
  }

  def run(spark: SparkSession, inputPath: String, outputPath: String): Unit = {

    val schema = StructType(Seq(
      StructField("OrderId", StringType),
      StructField("Title", StringType),
      StructField("Quantity", IntegerType),
      StructField("CreateTime", TimestampType)
    ))


    val df = spark.read.option("header", true).schema(schema).csv(inputPath)

    val frame = df.withColumn("Date", df("CreateTime").cast(DateType))
    frame.show()
    frame
      .groupBy("Title", "Date")
      .count()
      .withColumnRenamed("count", "Total")
      .select( "Date", "Title", "Total")
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv(outputPath)
  }
}
