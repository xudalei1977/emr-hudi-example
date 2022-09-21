package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HudiConfig, JsonSchema, Meta, SparkHelper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.slf4j.LoggerFactory
import org.apache.hudi.DataSourceReadOptions._
import java.time.LocalDateTime
import java.util.Date
import java.text.SimpleDateFormat
import scala.collection.mutable

object Hive2Hudi {

  private val log = LoggerFactory.getLogger("Hive2Hudi")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)

    // Set log4j level to warn
    Logger.getLogger("org").setLevel(Level.WARN)

    //System.setProperty("HADOOP_USER_NAME", "hadoop")
    val parmas = Config.parseConfig(Hive2Hudi, args)

    // init spark session
    val spark = SparkHelper.getSparkSession(parmas.env)
    val df = spark.read.format("parquet")
                  .load(s"${parmas.hudiBasePath}/${parmas.syncDB}/${parmas.syncTableName}")
                  .filter(_ != null)
                  .repartition(5000)

    writeHudiTable(df, parmas.syncDB, parmas.syncTableName, parmas.hudiWriteOperation, parmas.zookeeperUrl,
                  parmas.hudiKeyField, parmas.hudiCombineField, parmas.hudiPartition, parmas.hudiBasePath, "COPY_ON_WRITE")
  }

}

val df = spark.read.format("parquet").
              load(s"s3://dalei-demo/tpcds/data10g/customer").
              filter(df.c_last_review_date != null)
filter(_.last != null).
              repartition(5000)

writeHudiTable(df2, "tpcds_data100g_hudi", "customer_1", "upsert", "ip-10-0-0-147.ec2.internal",
  "c_customer_sk", "c_last_review_date", "", "s3://dalei-demo/hudi", "MERGE_ON_READ")


val df1 = df.withColumn("cc_division", col("cc_division") - lit(9))
val df1 = df.filter("c_last_review_date is not null")
val df2 = df1.withColumn("c_birth_year", lit(2000))

spark-shell --master yarn \
--deploy-mode client \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.sql.hive.convertMetastoreParquet=false" \
--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.10.0,org.apache.spark:spark-avro_2.12:3.1.2 \
  --jars s3://dalei-demo/jars/scopt_2.12-4.0.0-RC2.jar,/usr/share/aws/aws-java-sdk/aws-java-sdk-bundle-1.12.31.jar \
//  --class com.aws.analytics.Hive2Hudi s3://dalei-demo/jars/emr-hudi-example-1.0-SNAPSHOT.jar \
//  -e dev \
//  -g s3://dalei-demo/hudi \
//  -s tpcds_data10g_hudi -u household_demographics \
//  -z hd_demo_sk -q ws_sold_date_sk \
//  -l ip-10-0-0-124.ec2.internal


  import org.apache.hudi.QuickstartUtils._
  import org.apache.spark.sql.SaveMode._
  import org.apache.hudi.DataSourceReadOptions._
  import org.apache.hudi.DataSourceWriteOptions._
  import org.apache.hudi.config.HoodieWriteConfig._
  import java.util.Date

  val tableName = "store_sales1"
  val basePath = "s3://dalei-demo/hudi/tpcds_hudi_cluster/store_sales1"
  val partitionKey = "ss_sold_date_sk"

  val df = spark.read.format("parquet").
                load(s"s3://dalei-demo/tpcds/data10g/store_sales").
                filter("ss_sold_time_sk is not null and ss_item_sk is not null and ss_sold_date_sk=2450817 and ss_customer_sk is not null").
                withColumn("ts", lit((new Date()).getTime)).
                repartition(1000)

  df1.write.format("org.apache.hudi").
          option(TABLE_NAME, tableName).
          option("hoodie.datasource.write.precombine.field", "ts").
          option("hoodie.datasource.write.recordkey.field", "ss_sold_time_sk, ss_item_sk").
          option("hoodie.datasource.write.partitionpath.field", partitionKey).
          option("hoodie.upsert.shuffle.parallelism", "100").
          option("hoodie.datasource.write.table.type", "MERGE_ON_READ").
          option("hoodie.datasource.write.operation", "upsert").
          option("hoodie.parquet.max.file.size", "10485760").
          option("hoodie.datasource.write.hive_style_partitioning", "true").
          option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator").
//          option("hoodie.datasource.hive_sync.enable", "true").
//          option("hoodie.datasource.hive_sync.mode", "hms").
//          option("hoodie.datasource.hive_sync.database", "tpcds_hudi_cluster").
//          option("hoodie.datasource.hive_sync.table", tableName).
//          option("hoodie.datasource.hive_sync.partition_fields", partitionKey).
          option("hoodie.parquet.small.file.limit", "0").
          option("hoodie.clustering.inline", "true").
          option("hoodie.clustering.inline.max.commits", "2").
          option("hoodie.clustering.plan.strategy.max.num.groups", "10000").
          option("hoodie.clustering.plan.strategy.target.file.max.bytes", "1073741824").
          option("hoodie.clustering.plan.strategy.small.file.limit", "629145600").
          option("hoodie.clustering.plan.strategy.sort.columns", "ss_customer_sk").
          mode(Append).
          save(basePath);

  val df1 = spark.read.format("hudi").option("hoodie.datasource.query.type", "snapshot").
                  load("s3://dalei-demo/hudi/tpcds_hudi_nocluster/store_sales").
                  filter("ss_sold_date_sk=2450816").
                  drop(col("_hoodie_commit_seqno")).drop(col("_hoodie_commit_time")).
                  drop(col("_hoodie_record_key")).drop(col("_hoodie_partition_path")).
                  drop(col("_hoodie_file_name"))

  val df2 = df1.withColumn("ss_ext_tax", col("ss_ext_tax") + lit(1.0))

  val df1 = spark.read.format("org.apache.hudi").
  option("hoodie.datasource.query.type", "read_optimized").
  load("s3://dalei-demo/hudi/tpcds_hudi_cluster/store_sales1").
  filter("ss_sold_date_sk=2450817")

  option("hoodie.datasource.read.begin.instanttime", "20220704152037").
  option("hoodie.datasource.read.end.instanttime", "20220702155555289").

  ss_sold_time_sk:50884,ss_item_sk:120158
  ss_sold_time_sk:50884,ss_item_sk:17674


  val tableName = "customer_address"
  val basePath = "s3://dalei-demo/hudi/tpcds_hudi_cluster/customer_address"

  val df = spark.read.format("parquet").
                  load(s"s3://dalei-demo/tpcds/data10g/customer_address").
                  filter("ca_address_sk is not null")

  df2.write.format("org.apache.hudi").
            option(TABLE_NAME, tableName).
            option("hoodie.datasource.write.precombine.field", "ca_address_id").
            option("hoodie.datasource.write.recordkey.field", "ca_address_sk").
            option("hoodie.upsert.shuffle.parallelism", "100").
            option("hoodie.datasource.write.table.type", "COPY_ON_WRITE").
            option("hoodie.datasource.write.operation", "upsert").
            option("hoodie.parquet.max.file.size", "10485760").
            option("hoodie.datasource.hive_sync.enable", "true").
            option("hoodie.datasource.hive_sync.mode", "hms").
            option("hoodie.datasource.hive_sync.database", "tpcds_hudi_cluster").
            option("hoodie.datasource.hive_sync.table", tableName).
            option("hoodie.parquet.small.file.limit", "0").
            option("hoodie.clustering.inline", "true").
            option("hoodie.clustering.inline.max.commits", "2").
            option("hoodie.clustering.plan.strategy.target.file.max.bytes", "1073741824").
            option("hoodie.clustering.plan.strategy.small.file.limit", "629145600").
            option("hoodie.clustering.plan.strategy.sort.columns", "").
            mode(Append).
            save(basePath);

  val df1 = spark.read.format("hudi").option("hoodie.datasource.query.type", "read_optimized").
                  load("s3://dalei-demo/hudi/tpcds_hudi_cluster/customer_address")
  val df2 = df1.withColumn("ca_gmt_offset", col("ca_gmt_offset") + lit(1.1))

  spark-shell \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --jars /usr/lib/hudi/hudi-spark3-bundle_2.12-0.9.0-amzn-1.jar

import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

val tableName = "hudi_trips_cow"
val basePath = "s3://dalei-demo/hudi/dev/hudi_trips_cow"
val dataGen = new DataGenerator

  val inserts = convertToStringList(dataGen.generateInserts(10))
  val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
  df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  option("hoodie.datasource.write.table.type", "MERGE_ON_READ").
  mode(Overwrite).
  save(basePath)

  val df1 = spark.read.format("org.apache.hudi").
  option("hoodie.datasource.query.type", "incremental").
  option("hoodie.datasource.read.begin.instanttime", "20220626150639485").
  load(basePath)

  df1.createOrReplaceTempView("hudi_trips_snapshot")

  val hudiIncDF1 = spark.read.format("org.apache.hudi").
  option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL).
  option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20220625150639485").
  option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, "20220629150639485").
  load("s3://dalei-demo/hudi/tpcds_hudi_nocluster/store_sales")

  dfs.nameservices
  dfs.ha.namenodes
  dfs.namenode.rpc-address.
  dfs.client.failover.proxy.provider.
