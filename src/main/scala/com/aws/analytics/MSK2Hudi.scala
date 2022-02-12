package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HudiConfig, Meta, SparkHelper, JsonSchema}
import net.heartsavior.spark.KafkaOffsetCommitterListener
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
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


object MSK2Hudi {

  private val log = LoggerFactory.getLogger("msk2hudi")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)

    // Set log4j level to warn
    Logger.getLogger("org").setLevel(Level.WARN)

    //System.setProperty("HADOOP_USER_NAME", "hadoop")
    val parmas = Config.parseConfig(MSK2Hudi, args)

    // init spark session
    val ss = SparkHelper.getSparkSession(parmas.env)
    import ss.implicits._
    val df = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", parmas.brokerList)
      .option("subscribe", parmas.sourceTopic)
      .option("startingOffsets", parmas.startPos)
      .option("failOnDataLoss", false)
      .option("maxOffsetsPerTrigger", "100")
//      .option("kafka.consumer.commit.groupid", parmas.consumerGroup)
      .load()
      .repartition(Integer.valueOf(parmas.partitionNum))


    //process the schema from the first record from kafka
    var schema: StructType = null
    while(schema == null){
      df.withColumn("json", col("value").cast(StringType))
        .select("json").filter(_ != null)
        .writeStream
        .option("checkpointLocation", "/tmp/hudi/checkpoint/")
        .trigger(Trigger.Once())
        .format("hdfs://tmp/output.txt")
        .start()

      val tmpDf = ss.read.text("/tmp/json_string.txt")
      schema = JsonSchema.getCDCJsonSchemaFromJSONString(tmpDf.first().getAs[String](0))

      if(schema != null) {
        val hudiDf = tmpDf.select(from_json(col("value"), schema) as "data")
          .select("data.*")

        write2HudiFromDF(hudiDf)

      }
    }


    val query = df.withColumn("json", col("value").cast(StringType))
      .select(from_json(col("json"), schema) as "data")
      .select("data.*")
      .where("data.ts is not null")
      .writeStream
      .queryName("MSK2Hudi")
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        write2HudiFromDF(batchDF)
      }
      .option("checkpointLocation", "/tmp/hudi/checkpoint/")
      .trigger(Trigger.ProcessingTime(parmas.trigger + " seconds"))
      .start()
    df.take(1)(0)
    query.awaitTermination()
  }


  def write2HudiFromDF(batchDF: DataFrame) = {
    val newsDF = batchDF.filter(_ != null)
    if (!newsDF.isEmpty) {
      newsDF.persist()
      newsDF.show()

      println(LocalDateTime.now() + " === start writing table")
      newsDF.write.format("hudi")
          .option(TABLE_TYPE_OPT_KEY, "MERGE_ON_READ")
          .option(OPERATION_OPT_KEY, "")
          .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator")
          .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
          .option("hoodie.cleaner.policy.failed.writes", "LAZY")
          .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider")
          .option("hoodie.write.lock.zookeeper.url", "ip-10-0-0-51.ec2.internal")
          .option("hoodie.write.lock.zookeeper.port", "2181")
          .option("hoodie.write.lock.zookeeper.lock_key", "hudi_trips_cow_1")
          .option("hoodie.write.lock.zookeeper.base_path", "/hudi/write_lock")
          .option(PRECOMBINE_FIELD_OPT_KEY, "ts")
          .option(RECORDKEY_FIELD_OPT_KEY, "uuid")
          .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
          .option(ASYNC_COMPACT_ENABLE_OPT_KEY, "true")
          .option(HIVE_SUPPORT_TIMESTAMP, "true")
          .option(INLINE_COMPACT_NUM_DELTA_COMMITS_PROP, "1")
          .option(DataSourceWriteOptions.TABLE_NAME.key(), "hudi_trips_cow_1")
          .mode(SaveMode.Append)
          .save("s3://dalei-demo/hudi/hudi_trips_cow_1/")

      newsDF.unpersist()
      println(LocalDateTime.now() + " === finish")
    }
  }

}
