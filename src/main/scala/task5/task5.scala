package task5

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import task0.PageViewUserFollowByDateMonthForEachDomains.pcSchema

object task5 extends App {
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

  val formattedKenh14 = kenh14
    .withColumn("dt", to_timestamp(col("dt"), "yyyy-MM-dd HH:mm:ss"))

  val formattedKenh14WithDayOfWeek = formattedKenh14
    .withColumn("dayOfWeek", date_format(col("dt"), "E"))

  val pageViewsByDayOfWeek = formattedKenh14WithDayOfWeek.groupBy("dayOfWeek")
    .agg(
      count("*").as("pageviews"),
    )
    .sort(col("dayOfWeek"))
  pageViewsByDayOfWeek.show()

  ////////////////////////////////////////////////////////////////////////////////////////////// 2
  val pageviewsByMonth = formattedKenh14
    .groupBy(date_format(col("dt"), "yyyy-MM").alias("month"))
    .agg(
      count("*").alias("pageviews")
    )
    .sort(col("month"))
  pageviewsByMonth.show()

  ////////////////////////////////////////////////////////////////////////////////////////////// 3
  val usersByDayOfWeek = formattedKenh14WithDayOfWeek.groupBy("dayOfWeek")
    .agg(
      countDistinct("guid").as("users")
    )
    .sort(col("dayOfWeek"))
  usersByDayOfWeek.show()

  ////////////////////////////////////////////////////////////////////////////////////////////// 4
  val usersByMonth = formattedKenh14
    .groupBy(date_format(col("dt"), "yyyy-MM").alias("month"))
    .agg(
      countDistinct(col("guid")).alias("users")
    )
    .sort(col("month"))
  usersByMonth.show()

  ////////////////////////////////////////////////////////////////////////////////////////////// 5
  val location = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/khanhluong/Desktop/spark-job-scala/data/LocMapping.csv")

  // Inner join the pc and location DataFrames on loc_id
  val joinedDF = pc.join(location, Seq("loc_id"), "inner")

  // Group by location and calculate pageviews and users
  val resultDF = joinedDF.groupBy("loc_id", "Location")
    .agg(
      count("*").as("pageviews"),
      countDistinct("guid").as("users")
    ).sort(col("loc_id"))

  // Show the result or save it to your preferred storage
  resultDF.show(false)

  ////////////////////////////////////////////////////////////////////////////////////////////// 7
  val topCategories = pc.groupBy("category")
    .agg(
      countDistinct("guid").as("users"),
      count("*").as("pageviews")
    )
  topCategories.select("category","users").sort(desc("users")).limit(10).show()
  topCategories.select("category","pageviews").sort(desc("pageviews")).limit(10).show()

  ////////////////////////////////////////////////////////////////////////////////////////////// 8
  val topPaths = pc.groupBy("path")
    .agg(
      countDistinct("guid").as("users"),
      count("*").as("pageviews")
    )
  topPaths.select("path", "users").sort(desc("users")).limit(10).show()
  topPaths.select("path", "pageviews").sort(desc("pageviews")).limit(10).show()

  ////////////////////////////////////////////////////////////////////////////////////////////// 9
  // Define the date range (replace 'date1' and 'date2' with your desired date range)
  val date1 = "2023-01-01"
  val date2 = "2023-12-31"

  // Filter the DataFrame to include only records within the date range
  val filteredDF = pc.filter(col("dt").between(date1, date2))
  // Calculate the total number of days in the date range
  val totalDays = datediff(to_date(lit(date2)), to_date(lit(date1))) + 1

  // Group by 'domain' and calculate the sum of views
  val sumDF = filteredDF.groupBy("domain")
    .agg(count("*").as("sum_views"))

  // Calculate the average views based on the sum of views and the total number of days
  val avgDF = sumDF.withColumn("avg_views", col("sum_views") / totalDays)

  // Add a new column 'domain_category' based on the average views
  val categorizedDF = avgDF.withColumn("domain_category", when(col("avg_views") > 1000000, "big_domain")
    .when(col("avg_views") >= 100000 && col("avg_views") <= 1000000, "medium_domain")
    .otherwise("small_domain"))

  // Show the categorized result
  categorizedDF.show()
}
