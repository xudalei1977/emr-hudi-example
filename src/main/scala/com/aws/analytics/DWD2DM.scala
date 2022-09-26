package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory
import org.apache.hudi.DataSourceReadOptions._

import java.util.Date
import java.text.SimpleDateFormat


object DWD2DM {

  private val log = LoggerFactory.getLogger("DWD2DM")
  private val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS")

  def main(args: Array[String]): Unit = {

    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)
    val parmas = Config.parseConfig(DWD2DM, args)

    val spark = SparkHelper.getSparkSession(parmas.env)
    import spark.implicits._

    //process the initial store_sales_dm table, you can put this step out of this function.
    spark.read.format("hudi")
          .load("s3://dalei-demo/hudi/kudu_migration/inventory_dwd")
          .createOrReplaceTempView("inventory_dwd")

    spark.sql(s"""select i_brand, inv_date_sk, sum(inv_quantity_on_hand) as inv_quantity_on_hand_sum
                  |from inventory_dwd
                  |group by i_brand, inv_date_sk """.stripMargin)
      .createOrReplaceTempView("inventory_dm_init")

    val df_init = spark.sql(s"""select i_brand, inv_date_sk, ${(new Date()).getTime} as created_ts, inv_quantity_on_hand_sum
                 |from inventory_dm_init """.stripMargin)

    writeHudiTable(df_init, parmas.syncDB, parmas.syncTableName, "upsert", parmas.zookeeperUrl,
          "i_brand", "created_ts", "inv_date_sk", parmas.hudiBasePath, "MERGE_ON_READ")

    //get the last commit time.
    spark.read.format("hudi").
          load("s3://dalei-demo/hudi/kudu_migration/inventory_dm").
          createOrReplaceTempView("inventory_dm")

    val commits = spark.sql("select max(_hoodie_commit_time) as commitTime from inventory_dm").
      map(row => row.getString(0)).collect()

    var beginTime = commits(0) // commit time we are interested in
    var endTime: String = ""

    while(true){
      Thread.sleep(parmas.hudiIntervel)

      endTime = DATE_FORMAT.format(new Date())

      spark.read.format("hudi").
            option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL).
            option(BEGIN_INSTANTTIME.key(), beginTime).
            option(END_INSTANTTIME.key(), endTime).
            load("s3://dalei-demo/hudi/kudu_migration/inventory_dwd").
            createOrReplaceTempView("inventory_dwd_inc")

      spark.sql(s"""select i_brand, inv_date_sk, sum(inv_quantity_on_hand) as inv_quantity_on_hand_sum
                   |from inventory_dwd_inc
                   |group by i_brand, inv_date_sk """.stripMargin)
        .createOrReplaceTempView("inventory_dm_inc")

      spark.read.format("hudi").
          load("s3://dalei-demo/hudi/kudu_migration/inventory_dm").
          createOrReplaceTempView("inventory_dm")

      val df_inc = spark.sql(
        s"""select i.i_brand, i.inv_date_sk, ${(new Date()).getTime} as created_ts,
          | (i.inv_quantity_on_hand_sum + nvl(s.inv_quantity_on_hand_sum, 0) ) as inv_quantity_on_hand_sum
          | from inventory_dm_inc i left join inventory_dm s
          | on i.i_brand = s.i_brand and i.inv_date_sk = s.inv_date_sk""".stripMargin)

      writeHudiTable(df_inc, parmas.syncDB, parmas.syncTableName, "upsert", parmas.zookeeperUrl,
        "i_brand", "created_ts", "inv_date_sk", parmas.hudiBasePath, "MERGE_ON_READ")

      beginTime = endTime
    }
  }
}

//spark-submit --master yarn \
// --deploy-mode client \
// --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 4G --num-executors 2 \
//  --jars /usr/lib/hudi/hudi-spark3-bundle_2.12-0.11.0-amzn-0.jar,s3://dalei-demo/jars/ImpalaJDBC41.jar,s3://dalei-demo/jars/scopt_2.12-4.0.0-RC2.jar,/usr/lib/spark/external/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/external/lib/spark-streaming-kafka-0-10-assembly.jar,/usr/lib/hudi/cli/lib/kafka-clients-2.4.1.jar,s3://dalei-demo/jars/commons-pool2-2.6.2.jar \
//  --class com.aws.analytics.DWD2DM ./emr-hudi-example-1.0-SNAPSHOT.jar \
//  -e prod \
//  -l ip-10-0-0-121.ec2.internal \
//  -g s3://dalei-demo/hudi -s kudu_migration -u inventory_dm \
//  -i 300000
