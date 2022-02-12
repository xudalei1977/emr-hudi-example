package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HudiConfig, Meta, SparkHelper}
import net.heartsavior.spark.KafkaOffsetCommitterListener
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.functions.{col, from_json, lit}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType, TimestampType, StructField, StructType}
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.config.HoodieCompactionConfig._


object KDS2Hudi {

  private val log = LoggerFactory.getLogger("kds2hudi")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)

    // Set log4j level to warn
    Logger.getLogger("org").setLevel(Level.WARN)

    //System.setProperty("HADOOP_USER_NAME", "hadoop")
    val parmas = Config.parseConfig(KDS2Hudi, args)

    // init spark session
    val ss = SparkHelper.getSparkSession(parmas.env)
    import ss.implicits._

    val dataSchema = StructType(Seq(
                        StructField("id", IntegerType, true),
                        StructField("status", IntegerType, true),
                        StructField("age", IntegerType, true),
                        StructField("phone", LongType, true),
                        StructField("email", StringType, true),
                        StructField("ip", StringType, true),
                        StructField("cardDate", StringType, true),
                        StructField("creditCardNumber", StringType, true),
                        StructField("startAddress", StringType, true),
                        StructField("endAddress", StringType, true),
                        StructField("carNumber", StringType, true),
                        StructField("carType", StringType, true),
                        StructField("userName", StringType, true),
                        StructField("userID", StringType, true),
                        StructField("driverName", StringType, true),
                        StructField("driverRegisterDate", StringType, true),
                        StructField("score", DecimalType(4,2), true),
                        StructField("startLatitude", DecimalType(9, 7), true),
                        StructField("startLongitude", DecimalType(9, 7), true),
                        StructField("endLatitude", DecimalType(9, 7), true),
                        StructField("endLongitude", DecimalType(9, 7), true),
                        StructField("money", DecimalType(9, 2), true),
                        StructField("createTS", LongType, true),
                        StructField("eventTS", LongType, true)
    ))

    val metadataSchema = StructType(Seq(
                        StructField("commit-timestamp", TimestampType, true),
                        StructField("operation", IntegerType, true),
                        StructField("partition-key-type", IntegerType, true),
                        StructField("prev-transaction-id", LongType, true),
                        StructField("prev-transaction-record-id", StringType, true),
                        StructField("record-type", StringType, true),
                        StructField("schema-name", StringType, true),
                        StructField("stream-position", StringType, true),
                        StructField("table-name", StringType, true),
                        StructField("timestamp", TimestampType, true),
                        StructField("transaction-id", LongType, true),
                        StructField("transaction-record-id", IntegerType, true)
    ))

    val kinesisSchema = StructType(Seq(
                        StructField("data", dataSchema, true),
                        StructField("metadata", metadataSchema, true)
    ))

    val kinesisDF = ss
        .readStream
        .format("kinesis")
        .option("streamName", "demo")
        .option("initialPosition", "earliest")
        .option("region", "us-east-1")
        .load()


//    val dataDevicesDF = kinesisDF
//      .selectExpr("cast (data as STRING) jsonData").filter(_ != null)
//      .select(from_json(col("jsonData"), kinesisSchema).alias("taxi_order_cdc"))
//      .select("taxi_order_cdc.data.*")
//      .where("taxi_order_cdc.data is not null")
//      .writeStream
//      .format("console")
//      .option("checkpointLocation", "/tmp/hudi/checkpoint/")
//      .trigger(Trigger.ProcessingTime("10 seconds"))
//      .start()

    val dataDevicesDF = kinesisDF
      .selectExpr("cast (data as STRING) jsonData").filter(_ != null)
      .select(from_json(col("jsonData"), kinesisSchema).alias("taxi_order_cdc"))
      .select("taxi_order_cdc.data.*")
      .where("taxi_order_cdc.data is not null")
      .writeStream
      .queryName("KDS2Hudi")
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        val newsDF = batchDF.filter(_ != null)
        if (!newsDF.isEmpty) {
          newsDF.persist()
          //newsDF.show()

          println(LocalDateTime.now() + " === start writing table")
          newsDF.write.format("hudi")
            .option(TABLE_TYPE.key, "MERGE_ON_READ")
            .option(OPERATION.key, "upsert")
            .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator")
            .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
            .option("hoodie.cleaner.policy.failed.writes", "LAZY")
            .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider")
            .option("hoodie.write.lock.zookeeper.url", "ip-10-0-0-51.ec2.internal")
            .option("hoodie.write.lock.zookeeper.port", "2181")
            .option("hoodie.write.lock.zookeeper.lock_key", "hudi_trips_cow_1")
            .option("hoodie.write.lock.zookeeper.base_path", "/hudi/write_lock")
            .option(PRECOMBINE_FIELD.key, "ts")
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

      .option("checkpointLocation", "/tmp/hudi/checkpoint/")
      .trigger(Trigger.ProcessingTime(parmas.trigger + " seconds"))
      .start()

    dataDevicesDF.awaitTermination()

  }

}
