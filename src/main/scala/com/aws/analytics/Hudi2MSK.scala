package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HudiConfig, Meta, SparkHelper}
import net.heartsavior.spark.KafkaOffsetCommitterListener
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions.{from_avro, to_avro}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory



object Hudi2MSK {

  private val log = LoggerFactory.getLogger("hudi2msk")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)

    // Set log4j level to warn
    Logger.getLogger("org").setLevel(Level.WARN)

    //System.setProperty("HADOOP_USER_NAME", "hadoop")
    val parmas = Config.parseConfig(Hudi2MSK, args)

    // init spark session
    val ss = SparkHelper.getSparkSession(parmas.env)
    import ss.implicits._

    val df = ss
      .read
      .format("org.apache.hudi")
//      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
//      .option(INLINE_COMPACT_NUM_DELTA_COMMITS_PROP, "1")
//      .option(HIVE_STYLE_PARTITIONING_OPT_KEY, "true")
      .load("s3://dalei-demo/hudi/hudi_trips_cow/")
      .select(to_json(struct("begin_lat", "begin_lon", "driver", "end_lat", "end_lon", "fare", "rider", "ts", "uuid", "partitionpath")).as("value"))
      .selectExpr("cast(value as string)")

    val context = spark.sparkContext

    val ssc = new StreamingContext(context, Seconds(15))
    // 创建stream流
    val stream: InputDStream[ConsumerRecord[String, String]] =
      org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topics, getkafkaParms()))
    // 这里用flatmap把所有数组扁平化
    val filterRdd = stream.map(_.value()).flatMap(lines => {
      val buffer = new ListBuffer[Product]
      val asJsonArray = new JsonParser().parse(lines).getAsJsonArray
      for (index <- 0 until asJsonArray.size()) {
        val asJsonObject = asJsonArray.get(index).getAsJsonObject
        if (new objGetType().GetType(asJsonObject).equals("product")) {
          //把每一条明细，结构化为对象，方便转化为DF或者DSet
          buffer += new JSONToProduct().JSONToProduct(asJsonObject)
        }
      }
      buffer
    }).filter(r => r != null)

    filterRdd.foreachRDD(x=>x.foreach(y=>println(y.toString)))

    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", parmas.brokerList)
      .option("topic", parmas.sourceTopic)
      .save()
  }

}
