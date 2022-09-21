package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.SparkHelper
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date


object Kudu2Hudi {

  private val log = LoggerFactory.getLogger("Kudu2Hudi")
  private val IMPALA_JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver"
  private val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  private val INTERVAL = 60000

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)

    // Set log4j level to warn
    Logger.getLogger("org").setLevel(Level.WARN)

    //System.setProperty("HADOOP_USER_NAME", "hadoop")
    val parmas = Config.parseConfig(Kudu2Hudi, args)

    // init spark session
    val spark = SparkHelper.getSparkSession(parmas.env)

    log.info("******* impala url := " + parmas.impalaJdbcUrl)
    log.info("******* kudu master := " + parmas.kuduMaster)
    log.info("******* kudu database := " + parmas.kuduDatabase)

    // get all table in the database to migrate.
    val allTable = queryByJdbc(parmas.impalaJdbcUrl + parmas.kuduDatabase, "show tables")

    if(allTable != null && allTable.isInstanceOf[Seq[String]] && allTable.length > 0) {
      allTable.foreach( tableName => {
        log.info("******* table name := " + tableName)
        val (primaryKey, partitionKey) = getPrimaryAndPartitionKey(parmas.impalaJdbcUrl + parmas.kuduDatabase, tableName)
        log.info("******* primary key :=" + primaryKey)
        log.info("******* partition key :=" + partitionKey)

        val t1 = new Thread(new Task(spark, parmas, tableName, primaryKey, partitionKey))
        t1.start()
      })
    }
  }
}

object Task {
  private val log = LoggerFactory.getLogger("Kudu2Hudi")
}

class Task(val spark: SparkSession, val parmas: Config,
           val tableName: String, val primaryKey: String, val partitionKey: String) extends Runnable {
  override def run(): Unit = {
    val beginTime = (new Date).getTime

    val df = spark.read
      .option("kudu.master", parmas.kuduMaster)
      .option("kudu.table", "impala::" + parmas.kuduDatabase + "." + tableName)
      .format("kudu").load
      .withColumn("created_ts", lit((new Date()).getTime))
      .limit(10000)

    val tableType = if (partitionKey != null && partitionKey.length > 0) "MERGE_ON_READ" else "COPY_ON_WRITE"

    writeHudiTable(df, parmas.syncDB, tableName, "insert", parmas.zookeeperUrl,
      primaryKey, "created_ts", partitionKey, parmas.hudiBasePath, tableType)

    val endTime = (new Date).getTime
    Task.log.info("*********** Thread := " + Thread.currentThread + "***** use time := " + (endTime - beginTime))
  }
}

//spark-shell --master yarn \
//--deploy-mode client \
//--jars s3://dalei-demo/jars/kudu-client-1.16.0.jar,s3://dalei-demo/jars/kudu-spark3_2.12-1.16.0.jar,s3://dalei-demo/jars/ImpalaJDBC41.jar
//
//spark-shell --master yarn \
//  --deploy-mode client \
//  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
//  --conf "spark.sql.hive.convertMetastoreParquet=false" \
//  --packages org.apache.hudi:hudi-spark-bundle_2.11:0.10.0 \
//  --deploy-mode client \
//  --jars s3://dalei-demo/jars/kudu-client-1.10.0-cdh6.3.2.jar,s3://dalei-demo/jars/kudu-spark2_2.11-1.10.0-cdh6.3.2.jar,s3://dalei-demo/jars/ImpalaJDBC41.jar
