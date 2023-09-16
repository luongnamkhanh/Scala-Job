package task2

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import task0.PageViewUserFollowByDateMonthForEachDomains.pcSchema

object Kenh14Analysis extends App {
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

  /////////////////////////////////////////////////////////////// 1
  val pageView = kenh14.count()
  println(pageView)
  ///////////////////////////////////////////////////////////////

  val kenh14Formatted = kenh14
    .withColumn("dt", to_timestamp(col("dt"), "yyyy-MM-dd HH:mm:ss"))

  val kenh14FormattedWithHour = kenh14Formatted
    .withColumn("hourOfDay", hour(col("dt")))


  /////////////////////////////////////////////////////////////// 2
  val pageViewByHour = kenh14FormattedWithHour.groupBy("domain", "hourOfDay")
    .agg(
      count("*").as("pageviews"),
    )
    .sort(col("domain"), col("hourOfDay"))
  pageViewByHour.show(false)
  /////////////////////////////////////////////////////////////// 4

  val users = kenh14.dropDuplicates("guid").count()
  println(users)

  /////////////////////////////////////////////////////////////// 5
  val userByHour = kenh14FormattedWithHour.groupBy("domain", "hourOfDay")
    .agg(
      countDistinct("guid").as("user"),
    )
    .sort(col("domain"), col("hourOfDay"))
  userByHour.show(false)
  /////////////////////////////////////////////////////////////// 6

  val topPath = kenh14.groupBy("path")
    .agg(count("*").as("#"))
    .sort(desc("#"))
    .limit(1)
  topPath.show(false)

  /////////////////////////////////////////////////////////////// 7

  val pathUnder30View = kenh14.groupBy("path")
    .agg(count("*").as("#"))
    .where($"#" <= 30)
    .sort("#")
  pathUnder30View.show(false)
  /////////////////////////////////////////////////////////////// 8

  val totalPageViewOfUserHigher10PageView = kenh14.groupBy("guid")
    .agg(count("*").as("#PageView"))
    .where($"#PageView">10)
  val result = totalPageViewOfUserHigher10PageView.select(sum("#PageView").as("TotalPageViews"))
  result.show(false)

  /////////////////////////////////////////////////////////////// 9
  val topCategoriesByUsers = kenh14
    .groupBy("category")
    .agg(
      countDistinct("guid").as("users")
    )
    .sort(desc("users"))
    .limit(10)
  topCategoriesByUsers.show(false)

  /////////////////////////////////////////////////////////////// 10
  val topPathByPageView = kenh14
    .groupBy("path")
    .agg(
      first("category").as("category"),
      count("*").as("pageviews")
    )
  val joinPathCategory = topCategoriesByUsers.join(topPathByPageView,usingColumn = "category","inner").sort(desc("users"),desc("pageviews"))
  joinPathCategory.createOrReplaceTempView("kenh14")
  val result10 = spark.sql(
    "select kenh14.category, kenh14.users, kenh14.path, kenh14.pageviews " +
      "from kenh14 " +
      "inner join (" +
        "select category, max(pageviews) max_pageview " +
        "from kenh14 " +
        "group by category" +
      ") as max_pageviews " +
      "on kenh14.category = max_pageviews.category and kenh14.pageviews = max_pageviews.max_pageview " +
      "order by kenh14.users DESC"
  )
  result10.show(false)
}
