package task0

import org.apache.log4j._
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
object PageViewUserFollowByDateMonthForEachDomains {

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

    pc.printSchema()
    // Chuyển đổi cột 'dt' thành định dạng timestamp
    val formattedPc = pc.withColumn("dt", to_timestamp(pc("dt"), "yyyy-MM-dd HH:mm:ss"))

    // Tính pageview theo ngày và tháng cho từng domain
    val pageviewsByDay = formattedPc
      .groupBy(col("domain"), date_format(col("dt"), "yyyy-MM-dd").alias("date"))
      .agg(
        count("*").alias("pageviews")
      )
      .sort(col("domain"),col("date"))

    val pageviewsByMonth = formattedPc
      .groupBy(col("domain"), date_format(col("dt"), "yyyy-MM").alias("month"))
      .agg(
        count("*").alias("pageviews")
      )
      .sort(col("domain"),col("month"))

    // Tính số người dùng theo ngày và tháng cho từng domain
    val usersByDay = formattedPc
      .groupBy(col("domain"), date_format(col("dt"), "yyyy-MM-dd").alias("date"))
      .agg(
        countDistinct(col("guid")).alias("users")
      )
      .sort(col("domain"),col("date"))

    val usersByMonth = formattedPc
      .groupBy(col("domain"), date_format(col("dt"), "yyyy-MM").alias("month"))
      .agg(
        countDistinct(col("guid")).alias("users")
      )
      .sort(col("domain"),col("month"))

    pageviewsByDay.show(false)
    pageviewsByMonth.show(false)
    usersByDay.show(false)
    usersByMonth.show(false)

    spark.stop()
  }

}
