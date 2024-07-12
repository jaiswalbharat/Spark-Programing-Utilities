import org.apache.spark.sql.{SparkSession, DataFrame}

class HiveQueryExecutor(spark: SparkSession) extends QueryExecute {
  override def executeQuery(query: String, database: String): DataFrame = {
    spark.sql(s"USE $database")
    spark.sql(query)
  }
}
