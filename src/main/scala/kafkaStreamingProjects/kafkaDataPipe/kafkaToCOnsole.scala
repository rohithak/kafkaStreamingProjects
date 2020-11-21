package kafkaStreamingProjects.kafkaDataPipe

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import commonFunctions.createSparkSession
import kafkaStreamingProjects.kafakOperations.kafkaRead

object kafkaToCOnsole {

  val sparksess = createSparkSession.createSparkSess("kafkaToKafka", "local[2]")
  sparksess.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {

    val kafkaReadObj = new kafkaRead(sparksess, "localhost:9092")

    val kafkaDF: DataFrame = kafkaReadObj.topicToDataFrame("testTopic")

    kafkaDF
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

}