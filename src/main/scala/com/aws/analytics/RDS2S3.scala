package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HiveUtil, MySQLUtil, SparkHelper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, date_format, lit}
import org.slf4j.LoggerFactory


object RDS2S3 {

  private val log = LoggerFactory.getLogger("RDS2S3")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)

    val parmas = Config.parseConfig(RDS2S3, args)
    val spark = SparkHelper.getSparkSession(parmas.env)

    // get all table in the database to migrate.
    val mySQLUtil = new MySQLUtil()
    val hiveUtil = new HiveUtil()

    val allTable: Seq[String] = if (parmas.rdsTableName != "") Seq(parmas.rdsTableName)
                                else mySQLUtil.queryByJdbc(parmas, "show tables")

    if(allTable != null && allTable.isInstanceOf[Seq[String]] && allTable.length > 0) {
      allTable.foreach( tableName => {
        // create the hive table
        val tableDetail = mySQLUtil.getTableDetails(parmas, tableName)
        val (createTableScript, partitionOpt) = hiveUtil.getCreateTableString(tableDetail, parmas, tableName)
        val partitionCol = partitionOpt.getOrElse("")

        if (partitionCol != "") {
          log.info(s"************* createTableScript := $createTableScript")
          val ret = hiveUtil.execByJDBC(parmas, createTableScript)
          log.info(s"************* ret := $ret")
          if (ret) {
            // if create table, then insert the data
            try {
              //            val filteredDf = df.filter(date_format(col("last_update_time"), "yyyy-MM-dd").isin(dates:_*))
              //            filteredDf.show(false)
              //            filteredDf.printSchema()
              //
              //            filteredDf.withColumn("pt", lit(parmas.hivePartitionValue))
              //              .write
              //              .mode(SaveMode.Overwrite)
              //              .option("partition", s"pt=${parmas.hivePartitionValue}")
              //              .insertInto(s"${parmas.hiveDatabase}.$tableName")
              val df = spark.read.parquet(s"${parmas.rdsDataInS3}/${parmas.rdsDatabase}/$tableName")
              df.printSchema()
              df.repartition(parmas.partitionNum).createOrReplaceTempView(s"temp_$tableName")
              var insertSql = s"insert overwrite table ${parmas.hiveDatabase}.$tableName partition ($partitionCol='${parmas.hivePartitionValue}') " +
                s"select * from temp_$tableName"

              if (parmas.sourceDateRange != "")
                insertSql = insertSql + s" where date_format(last_update_time, 'yyyy-MM-dd') in (${parmas.sourceDateRange})"

              log.info(s"************* insertSql := $insertSql")
              // spark.sql(s"select * from temp_$tableName").show(false)
              spark.sql(insertSql)
            } catch {
              case e: Exception =>
                log.info(s"************* error table to check := $tableName")
            }
          }
        }
      })
    }
  }

  def getCurrentDate(): String= {
    val currentDate = java.time.LocalDate.now()
    val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")
    currentDate.format(formatter)
  }

}

//spark-submit --master yarn \
//  --deploy-mode client \
//  --driver-cores 4 --driver-memory 32G --executor-cores 4 --executor-memory 20G --num-executors 10 \
//  --conf "spark.memory.offHeap.enabled=true" \
//  --conf "spark.memory.offHeap.size=4g" \
//  --jars ./mysql-connector-java-8.0.28.jar,./scopt_2.12-4.0.0-RC2.jar \
//  --class com.aws.analytics.RDS2S3 ./emr-hudi-example-1.0-SNAPSHOT.jar \
//  -e prod -h emr-workshop-mysql8.chl9yxs6uftz.us-east-1.rds.amazonaws.com \
//  -d salesdb -t demo_partition -u admin -p HCserv1ce -s s3a://dalei-demo/hive -r "'2015-02-16','2015-04-13'" -n 20 \
//  -H ip-10-0-0-139.ec2.internal -D dev -P 2024-10-16 -S s3a://dalei-demo/hive
