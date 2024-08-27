import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

case class DataSplitter(spark: SparkSession) {

  /**
   * Function to sample the data maintaining the Type A and Type B ratios.
   */
  def sampleData(
    data: DataFrame, 
    sampleSize: Long, 
    typeARatio: Double, 
    typeBRatio: Double
  ): DataFrame = {
    
    val totalRatio = typeARatio + typeBRatio
    val typeAFraction = typeARatio / totalRatio
    val typeBFraction = typeBRatio / totalRatio

    val typeASample = data.filter(col("pollution") === "A")
                           .sample(withReplacement = false, typeAFraction)
    val typeBSample = data.filter(col("pollution") === "B")
                           .sample(withReplacement = false, typeBFraction)

    // Combine the samples, ensuring the final size matches the requested sample size
    val sampledData = typeASample.union(typeBSample).limit(sampleSize.toInt)
    sampledData
  }

  /**
   * Function to split the sampled data into train, test, and validation datasets while maintaining the type ratio.
   */
  def splitDataWithTypeRatio(
    data: DataFrame, 
    trainRatio: Double, 
    testRatio: Double, 
    validationRatio: Double, 
    typeARatio: Double, 
    typeBRatio: Double
  ): (DataFrame, DataFrame, DataFrame) = {

    // Separate the data into Type A and Type B
    val typeAData = data.filter(col("pollution") === "A")
    val typeBData = data.filter(col("pollution") === "B")

    // Split Type A data
    val Array(typeATrain, typeATest, typeAValidation) = typeAData.randomSplit(
      Array(trainRatio, testRatio, validationRatio), 
      seed = 12345
    )

    // Split Type B data
    val Array(typeBTrain, typeBTest, typeBValidation) = typeBData.randomSplit(
      Array(trainRatio, testRatio, validationRatio), 
      seed = 12345
    )

    // Combine Type A and Type B splits to form the final datasets
    val finalTrainData = typeATrain.union(typeBTrain)
    val finalTestData = typeATest.union(typeBTest)
    val finalValidationData = typeAValidation.union(typeBValidation)

    (finalTrainData, finalTestData, finalValidationData)
  }

  /**
   * Main processing function that combines sampling and splitting the data.
   */
  def process(
    data: DataFrame, 
    config: Map[String, Double]
  ): (DataFrame, DataFrame, DataFrame) = {

    // Sample the data
    val sampledData = sampleData(
      data,
      sampleSize = config("sampleSize").toLong,
      typeARatio = config("sampleTypeARatio"),
      typeBRatio = config("sampleTypeBRatio")
    )

    // Split the data into train, test, and validation sets while maintaining type ratios
    val (trainData, testData, validationData) = splitDataWithTypeRatio(
      sampledData,
      trainRatio = config("trainRatio"),
      testRatio = config("testRatio"),
      validationRatio = config("validationRatio"),
      typeARatio = config("splitTypeARatio"),
      typeBRatio = config("splitTypeBRatio")
    )

    (trainData, testData, validationData)
  }
}

object MainApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataSplitterApp")
      .getOrCreate()

    val data = spark.read.option("header", "true").csv("path/to/data.csv")

    val config = Map(
      "sampleSize" -> 3000000.0,  // 3 million rows sampled from 30 million rows
      "sampleTypeARatio" -> 1.4,  // Ratio in sample for Type A (1.4 million out of 2 million)
      "sampleTypeBRatio" -> 0.6,  // Ratio in sample for Type B (0.6 million out of 2 million)
      "trainRatio" -> 0.6,        // 60% of sampled data for training
      "testRatio" -> 0.2,         // 20% of sampled data for testing
      "validationRatio" -> 0.2,   // 20% of sampled data for validation
      "splitTypeARatio" -> 0.7,   // Desired ratio in train/test/validation for Type A
      "splitTypeBRatio" -> 0.3    // Desired ratio in train/test/validation for Type B
    )

    val dataSplitter = DataSplitter(spark)
    val (trainData, testData, validationData) = dataSplitter.process(data, config)

    // Optionally, save the datasets
    trainData.write.mode("overwrite").csv("path/to/save/train_data")
    testData.write.mode("overwrite").csv("path/to/save/test_data")
    validationData.write.mode("overwrite").csv("path/to/save/validation_data")
  }
}
