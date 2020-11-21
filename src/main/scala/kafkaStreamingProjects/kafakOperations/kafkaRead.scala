package kafkaStreamingProjects.kafakOperations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import commonFunctions.createSparkSession

class kafkaRead(sparkSess: SparkSession, address: String) {
  def topicToDataFrame(topicName: String): DataFrame = {
    val kafkaDF = sparkSess.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", address)
      .option("subscribe", topicName)
      .load()
    return kafkaDF
  }
}