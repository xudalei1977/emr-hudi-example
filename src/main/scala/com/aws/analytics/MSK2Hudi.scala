package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HudiConfig, Meta, SparkHelper, JsonSchema}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.functions.{col, from_json, lit}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory
import org.apache.hudi.DataSourceReadOptions._

import java.time.LocalDateTime
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import java.time.LocalDateTime
import java.util.UUID
import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.config.HoodieCompactionConfig._
import org.apache.spark.sql.Row
import java.util.Date



object MSK2Hudi {

  private val log = LoggerFactory.getLogger("msk2hudi")

  //private val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)

    // Set log4j level to warn
    Logger.getLogger("org").setLevel(Level.WARN)

    //System.setProperty("HADOOP_USER_NAME", "hadoop")
    val parmas = Config.parseConfig(MSK2Hudi, args)

    // init spark session
    val ss = SparkHelper.getSparkSession(parmas.env)
//    ss.conf.set("spark.hadoop.validateOutputSpecs", "false")

    import ss.implicits._
    val df = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", parmas.brokerList)
      .option("subscribe", parmas.sourceTopic)
      .option("startingOffsets", parmas.startPos)
      .option("failOnDataLoss", false)
//      .option("maxOffsetsPerTrigger", "100")
//      .option("kafka.consumer.commit.groupid", parmas.consumerGroup)
      .load()
      .repartition(Integer.valueOf(parmas.partitionNum))

    //process the schema from the first record from kafka
    var schema: StructType = null
    var timeStamp: Long = 0L

    while(schema == null){
      timeStamp = (new Date).getTime
      df.withColumn("json", col("value").cast(StringType))
        .select("json").filter(_ != null)
        .writeStream
        .option("checkpointLocation", "/home/hadoop/checkpoint-once/")
        .trigger(Trigger.Once())
        .foreachBatch { (batchDF: DataFrame, _: Long) =>
          batchDF.write.text("/tmp/cdc-" + timeStamp)
        }
        .start()

      //waiting for the hdfs writing.
      Thread.sleep(30000)

      try {
        val tmpDf = ss.read.text("/tmp/cdc-" + timeStamp)
        if (tmpDf.count > 0) {
          val jsonString = tmpDf.first().getAs[String](0)
          schema = JsonSchema.getJsonSchemaFromJSONString(jsonString)
        }
      } catch {
        case e: Exception => log.warn("could not read the hdfs file.")
      }

    }

    val broadCastSchema = ss.sparkContext.broadcast(schema)

    val query = df.withColumn("json", col("value").cast(StringType))
      .select(from_json(col("json"), broadCastSchema.value) as "data")
      .select("data.*")
      .drop("__op")
      .drop("__source_connector")
      .drop("__source_db")
      .drop("__source_table")
      .drop("__source_file")
      .drop("__source_pos")
      .drop("__source_ts_ms")
      .drop("__deleted")
      .where("data.id is not null")
      .writeStream
      .queryName("MSK2Hudi")
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        write2HudiFromDF(batchDF, parmas)
      }
      .option("checkpointLocation", parmas.checkpointDir)
      .trigger(Trigger.ProcessingTime(parmas.trigger + " seconds"))
      .start()

    query.awaitTermination()
  }

  def write2HudiFromDF(batchDF: DataFrame, parmas: Config) = {
    val newsDF = batchDF.filter(_ != null)
    if (!newsDF.isEmpty) {
      newsDF.persist()
    //  newsDF.show()

      println(LocalDateTime.now() + " === start writing table")
      newsDF.write.format("hudi")
        .options(HudiConfig.getEventConfig(parmas))
        .mode(SaveMode.Append)
        .save(parmas.hudiEventBasePath)

      newsDF.unpersist()
      println(LocalDateTime.now() + " === finish")
    }
  }

}
