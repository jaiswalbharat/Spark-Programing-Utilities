import org.apache.spark.sql.{SparkSession, DataFrame}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.io.Source
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer

case class QueryConfig(database: String, query: String, group_by_column: String, compare_iteration: Int, threshold_percentage: Double)

object Driver {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Query Executor")
      .enableHiveSupport()
      .getOrCreate()

    val jsonSource = Source.fromFile("path_to_json_config.json").mkString
    val queryConfigs = parse(jsonSource).extract[List[QueryConfig]]

    val queryExecutor = new HiveQueryExecutor(spark)
    val alert = new EmailAlert()
    val validator = new HiveTableValidation(spark)
    
    val breachDetailsList = ListBuffer[String]()

    queryConfigs.par.foreach { config =>
      if (validator.isValidTable(config.query)) {
        val result = queryExecutor.executeQuery(config.query, config.database)
        // Save the result in a Hive table
        result.write.mode("append").saveAsTable(s"${config.database}.results")

        // Load previous results for comparison
        val previousResults = spark.sql(s"SELECT * FROM ${config.database}.results ORDER BY date DESC LIMIT ${config.compare_iteration}")
        
        // Aggregate previous results
        val previousAggregated = previousResults.groupBy(config.group_by_column).agg(avg("row_count").alias("avg_row_count"))

        // Join current results with previous aggregated results
        val comparison = result.as("current")
          .join(previousAggregated.as("previous"), col("current." + config.group_by_column) === col("previous." + config.group_by_column))
          .selectExpr("current." + config.group_by_column, "current.row_count", "previous.avg_row_count")

        // Calculate percentage change
        val breached = comparison.withColumn("percentage_change", 
            ((col("current.row_count") - col("previous.avg_row_count")) / col("previous.avg_row_count")) * 100)
          .filter(col("percentage_change") > config.threshold_percentage)

        if (breached.count() > 0) {
          val breachDetails = breached.collect().map(row => s"${config.database}.${config.group_by_column} = ${row.getString(0)}: ${row.getDouble(3)}%").mkString(", ")
          breachDetailsList += s"Breach detected for query: ${config.query}. Details: $breachDetails"
        }
      }
    }

    // Send a single alert email if there are any breaches
    if (breachDetailsList.nonEmpty) {
      alert.sendAlert(breachDetailsList.mkString("\n"))
    }

    spark.stop()
  }
}
