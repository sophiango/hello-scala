
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("FirstApp")
      .master("local[*]")
      .getOrCreate()
    val data = Seq(("Alice", 23), ("Bob", 45))
    val df = spark.createDataFrame(data).toDF("Name", "Age")
    df.show()
    df.filter(df("Age") > 30).show()
    spark.stop()
  }
}

