package task0

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PageViewUserFollowByLoc {
  case class pcSchema(dt: String, cookietime: String, browser_code: String, browser_ver: String, os_code: String, os_version: String, ip: String,
                      loc_id: BigInt, domain: String, siteId: String, channelId: String, path: String, referer: String, guid: String, geo: String, org_ip: String,
                      pageloadId: String, screen: String, d_guid: String, category: String, utm_source: String, utm_campaign: String, utm_medium: String,
                      milis: String, ipV6: String)

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
      .parquet("/home/khanhluong/Desktop/spark-job-scala/data/pc.parquet")
      .as[pcSchema]

    //    pc.printSchema()
    import spark.implicits._
    val location = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/home/khanhluong/Desktop/spark-job-scala/data/LocMapping.csv")

    // Inner join the pc and location DataFrames on loc_id
    val joinedDF = pc.join(location, Seq("loc_id"), "inner")

    // Group by location and calculate pageviews and users
    val resultDF = joinedDF.groupBy("loc_id","Location")
      .agg(
        count("*").as("pageviews"),
        countDistinct("guid").as("users")
      ).sort(col("loc_id"))

    // Show the result or save it to your preferred storage
    resultDF.show(false)




    spark.stop()
  }
}
