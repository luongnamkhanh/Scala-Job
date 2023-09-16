package task0

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TopCategoryAndPathByUser {
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


    // Group by category and path, and calculate pageviews and users
    val resultDF = pc.groupBy("category", "path")
      .agg(
        countDistinct("guid").as("users")
      )

    // Calculate the top category and top path by users
    val topCategoriesByUsers = resultDF
      .groupBy("category")
      .agg(
        sum("users").as("total_users")
      )
      .sort(desc("total_users"))
      .limit(10)
    val topPathByUsers = resultDF.sort(desc("users")).select("path", "users").limit(10)

    // Show the top results
    topCategoriesByUsers.show(false)
    topPathByUsers.show(false)



    spark.stop()
  }
}
