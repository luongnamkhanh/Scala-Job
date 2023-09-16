package task6

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import task6.PercentPageViewUserByAge.pcFullSchema

object pcFullKenh14Analysis extends App{
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

  val kenh14 = pc.filter($"domain"==="kenh14.vn")

  val result = kenh14.groupBy("category")
    .agg(
      count("*").alias("pageviews"),
      countDistinct("guid").alias("users"),
      sum("tos").alias("tos"),
      sum("tor").alias("tor"),
      sum("top").alias("top")
    )
    .sort(desc("pageviews"))
  result.show()

  ///////////////////////////////////////////////////////////////////////////////// 2
  // Categorize age and gender
  val categorizedPC = kenh14.withColumn("age_category", when(col("age").between(18, 24), "18-24")
      .when(col("age").between(25, 34), "25-34")
      .when(col("age").between(35, 49), "35-49")
      .when(col("age") >= 50, "50+")
      .otherwise("Unknown"))

  // Merge age and gender categories into one
  val mergedCategories = categorizedPC.withColumn("age_gender_category",
    concat(col("gender"), lit("_"), col("age_category")))

  // Calculate the total count for each category
  val totalCategoryCount = mergedCategories.groupBy("category").agg(count("*").alias("total_count"))

  // Calculate the count and percentage by merged category and category
  val percentGenderAge = mergedCategories.groupBy("age_gender_category", "category")
    .agg(count("*").alias("count"))
    .join(totalCategoryCount, "category")
    .withColumn("percentage", col("count") *100/ col("total_count"))

  val percentGenderAge_result = percentGenderAge.select("category","age_gender_category","percentage")
  percentGenderAge_result.sort("category").show()
  // Pivot the data to have age-gender categories as columns
  val pivotedResult = percentGenderAge_result
    .groupBy("category")
    .pivot("age_gender_category")
    .agg(first("percentage"))
    .na.fill(0) // Fill any null values with 0
    .sort("category")

  pivotedResult.show()

  spark.stop()
}
