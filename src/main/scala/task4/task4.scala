package task4

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import task0.PageViewUserFollowByDateMonthForEachDomains.pcSchema

object task4 extends App{
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

  val kenh14 = pc.filter($"domain" === "kenh14.vn")

  // Group by User and calculate the number of pages per user
  val resultDf = kenh14
    .groupBy("guid")
    .agg(count("*").as("TotalPages"))

  // Calculate the average number of pages per user
  val averagePages = resultDf.agg(avg($"TotalPages")).first.getDouble(0)

  println(s"Average Pages per User on kenh14.vn: $averagePages")

  ////////////////////////////////////////////////////////////////////////////////// 2
  val formattedKenh14 = kenh14
    .groupBy("path")
    .agg(countDistinct("guid").as("TotalUsers"))
  val averageUsers = formattedKenh14.agg(avg($"TotalUsers")).first.getDouble(0)
  println(s"Average Users read a paper on kenh14.vn: $averageUsers")

  spark.stop()

}
