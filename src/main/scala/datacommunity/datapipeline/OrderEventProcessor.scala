package datacommunity.datapipeline

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object OrderEventProcessor {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .appName("Order Event Streaming Processor")
      .config("spark.driver.host", "127.0.0.1")
      .master("local")
      .getOrCreate()


    run(spark)
  }

  def run(spark: SparkSession): Unit = {
    val schema = StructType(Seq(
      StructField("timestamp", TimestampType),
      StructField("payload", StructType(Seq(
        StructField("orderId", StringType),
        StructField("items", ArrayType(StructType(Seq(
          StructField("title", StringType),
          StructField("quantity", IntegerType)
        ))))
      ))
      )
    ))
    val dataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "order_event")
      .option("startingOffsets", "latest")
      .load()

    import spark.implicits._

    dataFrame
      .withColumn("value", from_json($"value".cast("string"), schema))
      .select($"value.*")
      .withColumn("item", explode($"payload.items"))
      .withColumn("Title", $"item.title")
      .withColumn("Quantity", $"item.quantity")
      .select($"Title", $"Quantity", $"timestamp")
      .withWatermark("timestamp", "10 seconds")
      .groupBy(
        window($"timestamp", "10 seconds", "5 seconds"),
        $"Title"
      )
      .sum("Quantity")
      .writeStream
      .outputMode(OutputMode.Complete())
      //      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()

  }
}
