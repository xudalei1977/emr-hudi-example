package com.aws.analytics

import com.aws.analytics.cdc.CanalParser
import com.aws.analytics.cdc.const.HudiOP
import com.aws.analytics.cdc.util.JsonUtil
import com.aws.analytics.conf.{Config, TableInfo}
import com.aws.analytics.util.{HudiConfig, HudiWriteTask, Meta, SparkHelper}
import net.heartsavior.spark.KafkaOffsetCommitterListener
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.functions.{col, date_format, explode, from_json, lit, udf}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import java.time.format.DateTimeFormatter

object Parquet2AOS {

  case class TableInfoList(tableInfo: List[TableInfo])

  private val log = LoggerFactory.getLogger("Parquet2AOS")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)

    val params = Config.parseConfig(Parquet2AOS, args)

    // init spark session
    val ss = SparkHelper.getSparkSession(params.env)
    import ss.implicits._
    val df = ss.sql(s"select inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, inv_date_sk from tpcds_data500g.inventory limit 10")
    val rdd = df.rdd.map{ case row =>
      val jsonStr = "{\"inv_item_sk\": \"" + row.getAs[String]("inv_item_sk") + "\", " +
                    " \"inv_warehouse_sk\": \"" + row.getAs[String]("inv_warehouse_sk") + "\", " +
                    " \"inv_quantity_on_hand\": \"" + row.getAs[String]("inv_quantity_on_hand") + "\", " +
                    " \"inv_date_sk\": \"" + row.getAs[String]("inv_date_sk") + "\"}"

      println(jsonStr)

      ss.sparkContext.hadoopConfiguration.
    }
  }

  def postResponse(url: String, params: String = null, header: String = null): String ={
    val httpClient = HttpClients.createDefault()
    val post = new HttpPost(url)

    if (header != null) {
      val json = JSON.parseObject(header)
      json.keySet().toArray.map(_.toString).foreach(key => post.setHeader(key, json.getString(key)))
    }

    if (params != null) {
      post.setEntity(new StringEntity(params, "UTF-8"))
    }

    val response = httpClient.execute(post)    // 创建 client 实例
    EntityUtils.toString(response.getEntity, "UTF-8")   // 获取返回结果
  }

}
