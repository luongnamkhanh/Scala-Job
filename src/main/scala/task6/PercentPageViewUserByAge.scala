package task6

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PercentPageViewUserByAge {
  case class pcFullSchema(dt: String, cookietime: String, browser_code: String, browser_ver: String, os_code: String, os_version: String, ip: String,
                          loc_id: BigInt, domain: String, siteId: String, channelId: String, path: String, referer: String, guid: String, geo: String, org_ip: String,
                          pageloadId: String, screen: String, d_guid: String, category: String, utm_source: String, utm_campaign: String, utm_medium: String,
                          milis: String, ipV6: String, age: BigInt, gender: String, tor: BigInt, tos: BigInt, top: BigInt)

  def main(args: Array[String]) {
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

    pc.printSchema()
    val age_range = pc.withColumn("age_range", when (col("age")<18,"Under 18")
      .when(col("age")<=24,"From 18 to 24")
      .when(col("age")<=34,"From 25 to 34")
      .when(col("age")<=50,"From 35 to 50")
      .otherwise("Over 50")
    )
    val resultDF = age_range.groupBy("age_range")
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
}
