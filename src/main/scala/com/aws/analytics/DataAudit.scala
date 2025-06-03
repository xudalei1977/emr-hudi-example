package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HiveUtil, MySQLUtil, SparkHelper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, date_format, lit}
import org.slf4j.LoggerFactory

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}


object DataAudit {

  private val log = LoggerFactory.getLogger("DataAudit")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)

    val parmas = Config.parseConfig(DataAudit, args)
    val spark = SparkHelper.getSparkSession(parmas.env)

    val file = new File(parmas.auditResult)

    // get all table in the database to audit.
    val hiveUtil = new HiveUtil()

    val allTable: Seq[String] = if (parmas.rdsTableName != "") Seq(parmas.rdsTableName)
                                else hiveUtil.queryByJDBC(parmas, "show tables")

    if(allTable != null && allTable.isInstanceOf[Seq[String]] && allTable.length > 0) {
      allTable.foreach( tableName => {
        // generate the audit sql
        val auditSQL = hiveUtil.generateAuditSQL(parmas, tableName)

        log.info(s"************* auditSQL := $auditSQL")
        val df = spark.sql(auditSQL)
        val columnNames = df.columns.mkString(",")

        val firstRow = df.first()
        val firstRowValues = firstRow.toSeq.map(_.toString).mkString(",")

        write2File(s"$tableName\n$columnNames\n$firstRowValues", file)
      })
    }
  }

  private def write2File(ret: String, file: File): Unit = {
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"))

    bw.write(ret)
    bw.write("\n")
    bw.close()
  }

}

//spark-submit --master yarn \
//  --deploy-mode client \
//  --driver-cores 2 --driver-memory 16G --executor-cores 2 --executor-memory 12G --num-executors 4 \
//  --jars ./scopt_2.12-4.0.0-RC2.jar \
//  --class com.aws.analytics.DataAudit ./emr-hudi-example-1.0-SNAPSHOT.jar \
//  -e prod -H localhost \
//  -D mc_2_hive -F dh='2024030612' -R /home/hadoop/audit_result.csv
