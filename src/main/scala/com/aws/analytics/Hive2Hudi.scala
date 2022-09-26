package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import java.util.Date

object Hive2Hudi {

  private val log = LoggerFactory.getLogger("Hive2Hudi")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)

    val parmas = Config.parseConfig(Hive2Hudi, args)

    // init spark session
    val spark = SparkHelper.getSparkSession(parmas.env)
    val df = spark.read.format("parquet")
                  .load(s"${parmas.hiveTablePath}")
                  .limit(20)
                  .filter(_ != null)
                  .filter(genPrimaryKeyFilter(parmas.hudiKeyField))
                  .withColumn(s"${parmas.hudiPartition}", col(s"${parmas.hudiPartition}").cast(StringType))
                  .withColumn("created_ts", lit((new Date()).getTime))
                  .repartition(Integer.valueOf(parmas.partitionNum))

    val tableType = if (parmas.hudiPartition != null && parmas.hudiPartition.length > 0) "MERGE_ON_READ" else "COPY_ON_WRITE"

    writeHudiTable(df, parmas.syncDB, parmas.syncTableName, "upsert", parmas.zookeeperUrl,
                  parmas.hudiKeyField, "created_ts", parmas.hudiPartition, parmas.hudiBasePath, tableType)
  }
}

//spark-submit --master yarn \
//  --deploy-mode client \
//  --jars /usr/lib/hudi/hudi-spark3-bundle_2.12-0.11.0-amzn-0.jar,s3://dalei-demo/jars/scopt_2.12-4.0.0-RC2.jar,/usr/lib/spark/external/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/external/lib/spark-streaming-kafka-0-10-assembly.jar,/usr/lib/hudi/cli/lib/kafka-clients-2.4.1.jar,s3://dalei-demo/jars/commons-pool2-2.6.2.jar \
//  --class com.aws.analytics.Hive2Hudi ./emr-hudi-example-1.0-SNAPSHOT.jar \
//  -e prod \
//  -l ip-10-0-0-121.ec2.internal \
//  -g s3://dalei-demo/hudi -s kudu_migration -u inventory -z inv_item_sk,inv_warehouse_sk -q inv_date_sk \
//  -h s3://dalei-demo/tpcds/data10g/inventory

//spark-shell --master yarn \
//  --deploy-mode client \
//  --jars /usr/lib/hudi/hudi-spark3-bundle_2.12-0.11.0-amzn-0.jar,s3://dalei-demo/jars/scopt_2.12-4.0.0-RC2.jar,/usr/lib/spark/external/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/external/lib/spark-streaming-kafka-0-10-assembly.jar,/usr/lib/hudi/cli/lib/kafka-clients-2.4.1.jar,s3://dalei-demo/jars/commons-pool2-2.6.2.jar \
//  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer"
//
//val df = spark.read.format("parquet").
//          load("s3://dalei-demo/tpcds/data10g/inventory").
//          filter(_ != null).
//          filter("inv_date_sk='2450822' and inv_item_sk is not null and inv_warehouse_sk is not null").
//          withColumn("inv_date_sk", col("inv_date_sk").cast(StringType)).
//          withColumn("created_ts", lit((new Date()).getTime)).
//          limit(10).
//          repartition(20)
//
//writeHudiTable(df, "kudu_migration", "inventory", "upsert", "ip-10-0-0-121.ec2.internal",
//  "inv_item_sk,inv_warehouse_sk", "created_ts", "inv_date_sk", "s3://dalei-demo/hudi", "MERGE_ON_READ")
//
//val df1 = spark.read.format("hudi").
//  load("s3://dalei-demo/hudi/kudu_migration/inventory")