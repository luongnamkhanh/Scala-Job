package task3

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import task0.PageViewUserFollowByDateMonthForEachDomains.pcSchema

object Task3 extends App {
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

  ////////////////////////////////////////////////////////////////////////////// 1
  val kenh14AndSoha = pc.filter($"domain"==="kenh14.vn" or $"domain"==="soha.vn")
  val result1 = kenh14AndSoha.groupBy("guid")
    .agg(countDistinct("domain").as("#domain"))
    .where($"#domain"===2)
    .count()
  println(result1)
  ////////////////////////////////////////////////////////////////////////////// 2

  val kenh14 = pc.filter($"domain" === "kenh14.vn")
  val soha = pc.filter($"domain" === "soha.vn")

  val topUsersWhoReadBoth = kenh14.join(soha, Seq("guid"), "inner")
    .groupBy("guid")
    .agg(count("*").as("totalPageviews"))
    .orderBy(desc("totalPageviews"))
    .limit(10)

  topUsersWhoReadBoth.show(false)
  ////////////////////////////////////////////////////////////////////////////// 3

  val formattedKenh14 = kenh14.select("guid","dt","referer","path").sort("guid","dt")
  // Define a window specification for partitioning by GUID and ordering by row number
  val windowSpec = Window.partitionBy("guid").orderBy("dt")

  // Assign a group ID to consecutive paths for each GUID
  val dfWithGroup = formattedKenh14.withColumn("RowNum", row_number().over(windowSpec))
  // Create a new column that holds the next row's referer using the lag function
  val dfWithNextReferer = dfWithGroup.withColumn("next_referer", lead($"referer", 1).over(windowSpec))
  dfWithNextReferer.show()
  // Now, you can filter the rows where current_path matches next_referer
  val matchingRows = dfWithNextReferer.filter($"path" === $"next_referer")
  // Assign a group ID to consecutive paths for each GUID
  val dfWithGroupID = matchingRows.withColumn("GroupID", $"RowNum" - row_number().over(windowSpec))
  // Count the number of consecutive paths for each group
  val pathCounts = dfWithGroupID.groupBy("guid", "GroupID")
    .agg(count("*").as("PathCount"))

  // Find GUIDs with more than 5 consecutive paths
  val result = pathCounts.filter($"PathCount" > 5)
    .select("guid")
    .distinct()

  result.show()


}
