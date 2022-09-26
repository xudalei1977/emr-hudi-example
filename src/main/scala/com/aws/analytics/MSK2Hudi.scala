package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HudiConfig, JsonSchema, Meta, SparkHelper}
import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_json, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import java.util.Date


object MSK2Hudi {

  private val log = LoggerFactory.getLogger("MSK2Hudi")

  def main(args: Array[String]): Unit = {

    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)
    val parmas = Config.parseConfig(MSK2Hudi, args)

    // init spark session
    implicit val spark = SparkHelper.getSparkSession(parmas.env)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", parmas.brokerList)
      .option("subscribe", parmas.sourceTopic)
      .option("startingOffsets", parmas.startPos)
      .option("failOnDataLoss", false)
      .load()
      .repartition(Integer.valueOf(parmas.partitionNum))

    val schema = spark.read.format("hudi").load(s"${parmas.hudiBasePath}/${parmas.syncDB}/${parmas.syncTableName}").schema
    val schema_bc = spark.sparkContext.broadcast(schema)
    val tableType = if (parmas.hudiPartition != null && parmas.hudiPartition.length > 0) "MERGE_ON_READ" else "COPY_ON_WRITE"

    val query = df.writeStream
      .queryName("MSK2Hudi")
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        if(batchDF != null && (!batchDF.isEmpty) ){
          println(LocalDateTime.now() + " ****** schema := " + schema.printTreeString())
          val df = batchDF.withColumn("json", col("value").cast(StringType))
            .select(from_json(col("json"), schema_bc.value) as "data")
            .select("data.*")
            .withColumn("created_ts", lit((new Date()).getTime))
            .filter(genPrimaryKeyFilter(parmas.hudiKeyField))

          writeHudiTable(df, parmas.syncDB, parmas.syncTableName, "upsert", parmas.zookeeperUrl,
                          parmas.hudiKeyField, "created_ts", parmas.hudiPartition, parmas.hudiBasePath, tableType)
        }
      }
      .option("checkpointLocation", parmas.checkpointDir)
      .trigger(Trigger.ProcessingTime(parmas.trigger + " seconds"))
      .start

    query.awaitTermination()
  }

}

//spark-shell --master yarn \
//  --deploy-mode client \
//  --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 4G --num-executors 2 \
//  --jars /usr/lib/hudi/hudi-spark3-bundle_2.12-0.11.0-amzn-0.jar,s3://dalei-demo/jars/scopt_2.12-4.0.0-RC2.jar,/usr/lib/spark/external/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/external/lib/spark-streaming-kafka-0-10-assembly.jar,/usr/lib/hudi/cli/lib/kafka-clients-2.4.1.jar,s3://dalei-demo/jars/commons-pool2-2.6.2.jar \
//  --class com.aws.analytics.MSK2Hudi ./emr-hudi-example-1.0-SNAPSHOT.jar \
//  -e prod \
//  -b b-3.demomsk1.3blx6f.c24.kafka.us-east-1.amazonaws.com:9092,b-1.demomsk1.3blx6f.c24.kafka.us-east-1.amazonaws.com:9092,b-2.demomsk1.3blx6f.c24.kafka.us-east-1.amazonaws.com:9092 \
//  -t kudu_java_test -o earliest -p hudi-consumer-test-group-01 \
//  -l ip-10-0-0-121.ec2.internal \
//  -i 10 -c /home/hadoop/checkpoint -g s3://dalei-demo/hudi -s kudu_migration -u inventory -z inv_item_sk,inv_warehouse_sk -q inv_date_sk
