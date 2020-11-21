package kafkaStreamingProjects.kafakOperations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import commonFunctions.createSparkSession

class kafkaWrite(sparkSess: SparkSession, address: String) {
  def dataFrameToTopic(topicName: String, loadReadyDF: DataFrame) {
    loadReadyDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", address)
      .option("topic", topicName)
      .save()

  }
}