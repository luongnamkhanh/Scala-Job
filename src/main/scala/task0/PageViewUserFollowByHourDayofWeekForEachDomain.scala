package task0

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PageViewUserFollowByHourDayofWeekForEachDomain {
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

    // Chuyển đổi cột 'dt' thành định dạng timestamp
    val formattedPc = pc
      .withColumn("dt", to_timestamp(col("dt"), "yyyy-MM-dd HH:mm:ss"))
      .select("dt", "domain", "guid")


    // Extract hour of the day and day of the week
    val formattedPcWithHourandDay = formattedPc
      .withColumn("hourOfDay", hour(col("dt")))
      .withColumn("dayOfWeek",date_format(col("dt"), "E"))


    // Group by domain, hour, and day of the week, and count pageviews and users
    val resultDF = formattedPcWithHourandDay.groupBy("domain","hourOfDay", "dayOfWeek")
      .agg(
        count("*").as("pageviews"),
        countDistinct("guid").as("users")
      )
      .sort(col("domain"),col("dayOfWeek"),col("hourOfDay"))

    // Show the result or save it to your preferred storage
    resultDF.show(30,false)

    spark.stop()
  }
}
