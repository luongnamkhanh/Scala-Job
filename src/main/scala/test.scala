import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object test extends App {
  val spark = SparkSession
    .builder
    .appName("ConsecutivePaths")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Sample DataFrame (replace this with your actual data)
  val df = Seq(
    ("User1", "/path1", null),
    ("User1", "/path2", "/path1"),
    ("User1", "/path3", "/path2"),
    ("User1", "/path4", "/path3"),
    ("User1", "/path5", "/path6"),
    ("User1", "/path7", "/path6"),
    ("User2", "/path1", null),
    ("User2", "/path2", "/path1"),
    ("User2", "/path3", "/path2"),
    ("User2", "/path4", "/path3"),
    ("User2", "/path5", "/path4"),
    ("User1", "/path6", "/path5"),
    ("User2", "/path6", "/path5"),
    ("User2", "/path7", "/path6"),
    ("User2", "/path8", "/path7")
  ).toDF("GUID", "CurrentPath", "Referer")

  // Define a window specification for partitioning by GUID and ordering by row number
  val windowSpec = Window.partitionBy("GUID").orderBy("CurrentPath")

  // Assign a group ID to consecutive paths for each GUID
  val dfWithGroup = df.withColumn("RowNum", row_number().over(windowSpec))
  dfWithGroup.show()
  // Create a new column that holds the next row's referer using the lag function
  val dfWithNextReferer = dfWithGroup.withColumn("next_referer", lead($"Referer", 1).over(windowSpec))
  dfWithNextReferer.show()
  // Now, you can filter the rows where current_path matches next_referer
  val matchingRows = dfWithNextReferer.filter($"CurrentPath" === $"next_referer")

  // Show or perform further operations on matchingRows
  matchingRows.show()
  val dfWithGroupID = matchingRows.withColumn("GroupID", $"RowNum" - row_number().over(windowSpec))
  // Count the number of consecutive paths for each group
  val pathCounts = dfWithGroupID.groupBy("GUID", "GroupID")
    .agg(count("*").as("PathCount"))

  // Find GUIDs with more than 5 consecutive paths
  val result = pathCounts.filter($"PathCount" > 5)
    .select("GUID")
    .distinct()

  result.show()

  //  result.show()

  spark.stop()
}