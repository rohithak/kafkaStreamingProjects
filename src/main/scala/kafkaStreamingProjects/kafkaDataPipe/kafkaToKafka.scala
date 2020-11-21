package kafkaStreamingProjects.kafkaDataPipe

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import commonFunctions.createSparkSession
import kafkaStreamingProjects.kafakOperations.kafkaWrite
import kafkaStreamingProjects.kafakOperations.kafkaRead
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration.DurationInt
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.ForeachWriter

object kafkaToKafka {

  val sparksess = createSparkSession.createSparkSess("kafkaToKafka", "local[2]")

  def main(args: Array[String]): Unit = {

    val kafkaReadObj = new kafkaRead(sparksess, "localhost:9092")
    val kafkaWriteObj = new kafkaWrite(sparksess, "localhost:9092")

    val kafkaDF = kafkaReadObj.topicToDataFrame("testTopic")

    val query = kafkaDF.writeStream.trigger(Trigger.ProcessingTime(1800L))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.persist()
        kafkaWriteObj.dataFrameToTopic("testTopic2", batchDF)
        batchDF.unpersist()
      }

    query.start()
      .awaitTermination()

  }

}