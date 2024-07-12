import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object SampleDataGenerator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Sample Data Generator")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val currentDate = LocalDate.now()
    
    // Create sample data for multiple dates
    val sampleData = (1 to 10).flatMap { i =>
      val date = currentDate.minusDays(i).format(formatter)
      (1 to 5).map { j =>
        ("group" + j, j * 10 + scala.util.Random.nextInt(10), date)
      }
    }.toDF("group_by_column", "row_count", "date")

    // Show sample data
    sampleData.show()

    // Save sample data to Hive table
    sampleData.write.mode("overwrite").saveAsTable("example_db.results")
    
    spark.stop()
  }
}
