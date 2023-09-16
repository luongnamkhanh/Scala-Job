package task6

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import task6.PercentPageViewUserByAge.pcFullSchema

object PercentPageViewUserByGender extends App {
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
    .parquet("/home/khanhluong/Desktop/spark-job-scala/data/pc-full.parquet")
    .as[pcFullSchema]


  val resultDF = pc.groupBy("gender")
    .agg(
      count("*").alias("pageviews"),
      countDistinct("guid").alias("users")
    )
  // Calculate the total sum of 'pageviews' and 'users' across all age ranges
  val totalPageviews = resultDF.agg(sum("pageviews")).collect()(0).getLong(0)
  val totalUsers = resultDF.agg(sum("users")).collect()(0).getLong(0)

  // Calculate the percentage of 'pageviews' and 'users' for each age range
  val resultWithPercentage = resultDF.withColumn("pageviews_percentage", col("pageviews") / totalPageviews * 100)
    .withColumn("users_percentage", col("users") / totalUsers * 100)
  resultWithPercentage.show()
}
