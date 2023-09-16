package task1

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import task0.PageViewUserFollowByDateMonthForEachDomains.pcSchema

object NumberPageViewUserAccessWebsite extends App{
  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)


  val spark = SparkSession
    .builder
    .appName("SparkJob")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val pc = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .parquet("/home/khanhluong/Desktop/spark-job-scala/data/pc.parquet")
    .as[pcSchema]

  val formattedPc = pc
    .withColumn("dt", to_timestamp(col("dt"), "yyyy-MM-dd HH:mm:ss"))
    .select("dt", "domain", "guid")

  val usersByDay = formattedPc
    .groupBy(col("domain"), date_format(col("dt"), "yyyy-MM-dd").alias("date"))
    .agg(
      countDistinct(col("guid")).alias("users")
    )
    .sort(col("domain"),col("date"))

  val pageviewsByDay = formattedPc
    .groupBy(col("domain"), date_format(col("dt"), "yyyy-MM-dd").alias("date"))
    .agg(
      count("*").alias("pageviews")
    )
    .sort(col("domain"), col("date"))

  usersByDay.show(false)
  pageviewsByDay.show(false)

  spark.stop()

}
