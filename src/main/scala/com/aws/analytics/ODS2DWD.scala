package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory
import org.apache.hudi.DataSourceReadOptions._

import java.util.Date
import java.text.SimpleDateFormat


object ODS2DWD {

  private val log = LoggerFactory.getLogger("ODS2DWD")
  private val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS")

  def main(args: Array[String]): Unit = {

    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)
    val parmas = Config.parseConfig(ODS2DWD, args)

    val spark = SparkHelper.getSparkSession(parmas.env)
    import spark.implicits._

    var beginTime: String = ""
    var endTime: String = ""

    spark.read.format("hudi").
          load("s3://dalei-demo/hudi/kudu_migration/inventory_dwd").
          createOrReplaceTempView("inventory_dwd_snapshot")

    val commits = spark.sql("select max(_hoodie_commit_time) as commitTime from inventory_dwd_snapshot").
                        map(row => row.getString(0)).collect()
    beginTime = commits(0) // commit time we are interested in

    //use item as dimension table
    spark.read.format("hudi").
              load("s3://dalei-demo/hudi/kudu_migration/item").
              createOrReplaceTempView("item")

    while(true){
      Thread.sleep(parmas.hudiIntervel)

      endTime = DATE_FORMAT.format(new Date())

      log.info("***** beginTime := " + beginTime)
      log.info("***** endTime := " + endTime)

      spark.read.format("hudi").
            option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL).
            option(BEGIN_INSTANTTIME.key(), beginTime).
            option(END_INSTANTTIME.key(), endTime).
            load("s3://dalei-demo/hudi/kudu_migration/inventory").
            createOrReplaceTempView("inventory")

      val df = spark.sql(
        s"""select in.inv_item_sk, nvl(i.i_brand, 'N/A') as i_brand, in.inv_warehouse_sk, in.inv_date_sk,
          |nvl(in.inv_quantity_on_hand, 0) as inv_quantity_on_hand,
          |${(new Date()).getTime} as created_ts
          |from inventory in left join item i on in.inv_item_sk = i.i_item_sk """.stripMargin)

      if(df.count > 0)
        writeHudiTable(df, parmas.syncDB, parmas.syncTableName, "upsert", parmas.zookeeperUrl,
            "inv_item_sk,i_brand,inv_warehouse_sk", "created_ts", "inv_date_sk", parmas.hudiBasePath, "MERGE_ON_READ")

      beginTime = endTime
    }
  }
}

//spark-submit --master yarn \
//  --deploy-mode client \
//  --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 4G --num-executors 2 \
//  --jars /usr/lib/hudi/hudi-spark3-bundle_2.12-0.11.0-amzn-0.jar,s3://dalei-demo/jars/scopt_2.12-4.0.0-RC2.jar,/usr/lib/spark/external/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/external/lib/spark-streaming-kafka-0-10-assembly.jar,/usr/lib/hudi/cli/lib/kafka-clients-2.4.1.jar,s3://dalei-demo/jars/commons-pool2-2.6.2.jar \
//  --class com.aws.analytics.ODS2DWD ./emr-hudi-example-1.0-SNAPSHOT.jar \
//  -e prod \
//  -l ip-10-0-0-121.ec2.internal \
//  -g s3://dalei-demo/hudi -s kudu_migration -u inventory_dwd \
//  -i 300000
//
//val df = spark.read.format("hudi").
//    load("s3://dalei-demo/hudi/kudu_migration/inventory").
//    createOrReplaceTempView("inventory")

//writeHudiTable(df, "kudu_migration", "inventory_dwd", "upsert", "ip-10-0-0-121.ec2.internal",
//  "inv_item_sk,i_brand,inv_warehouse_sk", "created_ts", "inv_date_sk", "s3://dalei-demo/hudi", "MERGE_ON_READ")
