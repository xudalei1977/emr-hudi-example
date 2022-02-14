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
import java.util.Date
import java.text.SimpleDateFormat


object Aggregation {

  private val log = LoggerFactory.getLogger("aggregation")

  private val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)

    // Set log4j level to warn
    Logger.getLogger("org").setLevel(Level.WARN)

    //System.setProperty("HADOOP_USER_NAME", "hadoop")
    val parmas = Config.parseConfig(MSK2Hudi, args)

    // init spark session
    val ss = SparkHelper.getSparkSession(parmas.env)
    import ss.implicits._

    // init the aggregation table

    val increDF = ss.read.parquet(parmas.hudiEventBasePath).drop("").
      select()

    while(true){
      //get the dataframe of hudi
      val increDF = ss.read.format("hudi")
                    .option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL)
                    .option(BEGIN_INSTANTTIME.key(), "beginTime")
                    .option(END_INSTANTTIME.key(), "endTime")
                    .load(parmas.hudiEventBasePath)
                    .createOrReplaceTempView("incre_detail")

      ss.sql("select cardDate, status, count(id) as order_sum, sum(money) as money_sum from incre_detail")
        .createOrReplaceTempView("incre_summary")

      val taxiOrderSummaryDF = ss.read.format("hudi")
                                  .load(parmas.hudiEventBasePath)
        .createOrReplaceTempView("taxi_order_summary")

      ss.sql(
        """select i.cardDate, i.status, (i.order_sum + s.order_sum) as order_sum, (i.money_sum + s.money_sum) as money_sum
          |from incre_detail i join taxi_order_summary s on i.cardDate = s.cardDate and i.status = s.status""".stripMargin)

      //save the hudi table


      Thread.sleep(600000)
    }

  }

}
