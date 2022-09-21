package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HudiConfig, JsonSchema, Meta, SparkHelper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.slf4j.LoggerFactory
import org.apache.hudi.DataSourceReadOptions._
import org.apache.spark.sql.streaming.OutputMode

import java.util.Date
import java.text.SimpleDateFormat


object DWD2DM {

  private val log = LoggerFactory.getLogger("DWD2DM")
  private val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  private val INTERVAL = 60000

  def main(args: Array[String]): Unit = {

    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)
    val parmas = Config.parseConfig(DWD2DM, args)

    val spark = SparkHelper.getSparkSession(parmas.env)

    //process the initial store_sales_dm table
    spark.read.format("hudi")
          .load("s3://dalei-demo/hudi/kudu_migration/store_sales_dwd")
          .createOrReplaceTempView("store_sales_dwd")

    spark.sql(s"""select i_brand_id, ss_sold_date_sk, sum(ss_sales_price) as ss_sales_price_sum, sum(ss_quantity) as ss_quantity
                  |group by i_brand_id, ss_sold_date_sk
                  |from store_sales_dwd """.stripMargin)
      .createOrReplaceTempView("store_sales_dm_init")

    val df_init = spark.sql(s"""select i_brand_id, ss_sold_date_sk, ${(new Date()).getTime} as created_ts, ss_sales_price_sum, ss_quantity
                 |from store_sales_dm_init """.stripMargin)

    writeHudiTable(df_init, parmas.syncDB, parmas.syncTableName, "upsert", parmas.zookeeperUrl,
      "i_brand_id", "created_ts", "ss_sold_date_sk", parmas.hudiBasePath, "MERGE_ON_READ")

    var beginTime: String = DATE_FORMAT.format(new Date())
    var endTime: String = ""

    while(true){
      Thread.sleep(INTERVAL)

      endTime = DATE_FORMAT.format(new Date())

      spark.read.format("hudi")
        .option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(BEGIN_INSTANTTIME.key(), beginTime)
        .option(END_INSTANTTIME.key(), endTime)
        .load("s3://dalei-demo/hudi/kudu_migration/store_sales_dwd")
        .createOrReplaceTempView("store_sales_dwd_inc")

      spark.sql(s"""select i_brand_id, ss_sold_date_sk, sum(ss_sales_price) as ss_sales_price_sum, sum(ss_quantity) as ss_quantity
                   |group by i_brand_id, ss_sold_date_sk
                   |from store_sales_dwd_inc """.stripMargin)
        .createOrReplaceTempView("store_sales_dm_inc")

      spark.read.format("hudi")
        .load("s3://dalei-demo/hudi/kudu_migration/store_sales_dm")
        .createOrReplaceTempView("store_sales_dm")

      val df_inc = spark.sql(
        s"""select i.i_brand_id, i.ss_sold_date_sk, ${(new Date()).getTime} as created_ts,
          | (i.ss_sales_price_sum + s.ss_sales_price_sum) as ss_sales_price_sum, (i.ss_quantity + s.ss_quantity) as ss_quantity
          | from store_sales_dm_inc i join store_sales_dm s
          | on i.ss_sold_date_sk = s.ss_sold_date_sk and i.i_brand_id = s.i_brand_id""".stripMargin)

      writeHudiTable(df_inc, parmas.syncDB, parmas.syncTableName, "upsert", parmas.zookeeperUrl,
        "i_brand_id", "created_ts", "ss_sold_date_sk", parmas.hudiBasePath, "MERGE_ON_READ")

      beginTime = endTime
    }
  }
}

//spark-submit --master yarn \
//--deploy-mode client \
//--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.10.0,org.apache.spark:spark-avro_2.12:3.1.2 \
//  --jars s3://dalei-demo/jars/ImpalaJDBC41.jar,s3://dalei-demo/jars/scopt_2.12-4.0.0-RC2.jar,/usr/lib/spark/external/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/external/lib/spark-streaming-kafka-0-10-assembly.jar,/usr/lib/hudi/cli/lib/kafka-clients-2.4.1.jar,s3://dalei-demo/jars/commons-pool2-2.6.2.jar \
//  --class com.aws.analytics.DWD2DM s3://dalei-demo/jars/emr-hudi-example-1.0-SNAPSHOT.jar \
//  -e dev \
//  -b b-3.tesla.4yy9yf.c5.kafka.us-east-1.amazonaws.com:9092,b-1.tesla.4yy9yf.c5.kafka.us-east-1.amazonaws.com:9092,b-2.tesla.4yy9yf.c5.kafka.us-east-1.amazonaws.com:9092 \
//  -l ip-10-0-0-67.ec2.internal \
//  -g s3://dalei-demo/hudi -s kudu_migration -u store_sales_dm
