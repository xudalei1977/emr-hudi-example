package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HudiConfig, Meta, SparkHelper, JsonSchema}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.slf4j.LoggerFactory


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
      //      .option("subscribe", parmas.sourceTopic)
      .option("subscribePattern", parmas.sourceTopic)
      .option("startingOffsets", parmas.startPos)
      .option("failOnDataLoss", false)
      //      .option("maxOffsetsPerTrigger", "100")
      //      .option("kafka.consumer.commit.groupid", parmas.consumerGroup)
      .load()
      .repartition(Integer.valueOf(parmas.partitionNum))

    val query = df.writeStream
      .queryName("MSK2Hudi")
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        if(batchDF != null && (!batchDF.isEmpty) )
          writeMultiTable2HudiFromDF(batchDF, parmas.syncDB, "upsert", parmas.zookeeperUrl,
            parmas.hudiBasePath, parmas.impalaJdbcUrl, parmas.kuduDatabase)
      }
      .option("checkpointLocation", parmas.checkpointDir)
      .trigger(Trigger.ProcessingTime(parmas.trigger + " seconds"))
      .start

    query.awaitTermination()
  }

}

spark-shell --master yarn \
--deploy-mode client \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.sql.hive.convertMetastoreParquet=false" \
--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.10.0,org.apache.spark:spark-avro_2.12:3.1.2 \
  --jars s3://dalei-demo/jars/ImpalaJDBC41.jar,s3://dalei-demo/jars/scopt_2.12-4.0.0-RC2.jar,/usr/lib/spark/external/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/external/lib/spark-streaming-kafka-0-10-assembly.jar,/usr/lib/hudi/cli/lib/kafka-clients-2.4.1.jar,s3://dalei-demo/jars/commons-pool2-2.6.2.jar

spark-submit --master yarn \
  --deploy-mode client \
--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.10.0,org.apache.spark:spark-avro_2.12:3.1.2 \
  --jars s3://dalei-demo/jars/ImpalaJDBC41.jar,s3://dalei-demo/jars/scopt_2.12-4.0.0-RC2.jar,/usr/lib/spark/external/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/external/lib/spark-streaming-kafka-0-10-assembly.jar,/usr/lib/hudi/cli/lib/kafka-clients-2.4.1.jar,s3://dalei-demo/jars/commons-pool2-2.6.2.jar \
  --class com.aws.analytics.MSK2Hudi s3://dalei-demo/jars/emr-hudi-example-1.0-SNAPSHOT.jar \
  -e dev \
  -b b-3.tesla.4yy9yf.c5.kafka.us-east-1.amazonaws.com:9092,b-1.tesla.4yy9yf.c5.kafka.us-east-1.amazonaws.com:9092,b-2.tesla.4yy9yf.c5.kafka.us-east-1.amazonaws.com:9092 \
  -t kudu.* -o earliest -p hudi-consumer-test-group-01 \
  -l ip-10-0-0-4.ec2.internal \
  -i 10 -c /home/hadoop/checkpoint -g s3://dalei-demo/hudi -s kudu_migration -m 20 \
  -i jdbc:impala://10.0.0.16:21050/ \
  -d tpcds_data10g_kudu