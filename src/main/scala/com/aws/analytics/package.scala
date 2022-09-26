package com.aws

import com.aws.analytics.util.JsonSchema.getJsonSchemaFromJSONString
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.time.LocalDateTime
import scala.collection.mutable

package object analytics {
  private val CLASS_NAME = "com.cloudera.impala.jdbc41.Driver"

  def writeHudiTable(batchDF: DataFrame, database: String, tableName: String, writeMode: String = "upsert", zookeeperUrl: String,
                     primaryKeys: String, combineKey: String, partitionKey: String, basePath: String, tableType: String): Unit = {
    val hasZookeeperLock = true
    val newsDF = batchDF.filter(_ != null)

    val options = mutable.Map(
      "hoodie.datasource.write.table.type" -> tableType,
      "hoodie.table.name" -> tableName,
      "hoodie.datasource.write.operation" -> writeMode,
      "hoodie.datasource.write.recordkey.field" -> primaryKeys,
      "hoodie.datasource.write.precombine.field" -> combineKey,
      "hoodie.clean.async" -> "true",
      "hoodie.clean.automatic" -> "true",
      "hoodie.cleaner.commits.retained" -> "2",
      "hoodie.keep.min.commits" -> "3",
      "hoodie.keep.max.commits" -> "4",
      "hoodie.datasource.hive_sync.enable" -> "true",
      "hoodie.datasource.hive_sync.mode" -> "hms",
      "hoodie.datasource.hive_sync.database" -> database,
      "hoodie.datasource.hive_sync.table" -> tableName,
      "hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.DefaultHoodieRecordPayload"
    )

    if (hasZookeeperLock) {
      options ++= mutable.Map(
        "hoodie.write.concurrency.mode" -> "optimistic_concurrency_control",
        "hoodie.cleaner.policy.failed.writes" -> "LAZY",
        "hoodie.write.lock.provider" -> "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider",
        "hoodie.write.lock.zookeeper.url" -> zookeeperUrl,
        "hoodie.write.lock.zookeeper.port" -> "2181",
        "hoodie.write.lock.zookeeper.lock_key" -> tableName,
        "hoodie.write.lock.zookeeper.base_path" -> "/hudi/write_lock")
    }

    if (partitionKey != "") {
      options ++= mutable.Map(
        "hoodie.datasource.write.partitionpath.field" -> partitionKey,
        "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.ComplexKeyGenerator",
        "hoodie.datasource.write.hive_style_partitioning" -> "true",
        "hoodie.datasource.hive_sync.partition_fields" -> partitionKey,
        "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.MultiPartKeysValueExtractor"
      )

    } else {
      options ++= mutable.Map(
        "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.NonPartitionedExtractor"
      )
    }

    if (!newsDF.isEmpty) {
      newsDF.persist()
      newsDF.show()
      println(LocalDateTime.now() + " === start writing table")

      try {
        newsDF.write.format("hudi").options(options)
          .mode(SaveMode.Append)
          .save(s"$basePath/$database/$tableName")
      } catch {
        case e: Exception => e.printStackTrace
      }

      newsDF.unpersist()
      println(LocalDateTime.now() + " === finish")
    }
  }


  //get the kudu table primary key and partition key.
  def getPrimaryAndPartitionKey(impalaJdbcUrl: String, tableName: String) = {

    val tmpArr = queryByJdbc(impalaJdbcUrl, s"show create table $tableName")
    val tableMeta = tmpArr(0)

    //val lineRegex = s"""(.*)PARTITIONED BY \\((.*)\\)(.*)LOCATION '(.*)'(.*)""".r
    //val lineRegex(tmp1, partitionKey, tmp2, location, tmp3) = tableMeta

    val primaryKeyIndex = tableMeta.indexOf("PRIMARY KEY (")
    val primaryKey = {
      if(primaryKeyIndex > 0) {
        tableMeta.substring(primaryKeyIndex + "PRIMARY KEY (".length, tableMeta.indexOf(")", primaryKeyIndex + "PRIMARY KEY (".length))
      } else
        ""
    }.trim

    val partitionStr = tableMeta.substring(tableMeta.indexOf("PARTITIONED BY (") + "PARTITIONED BY (".length)
    val partitionIndex = partitionStr.indexOf("RANGE (")
    val partitionKey = {
      if(partitionIndex > 0) {
        partitionStr.substring(partitionIndex + "RANGE (".length, partitionStr.indexOf(")", partitionIndex + "RANGE (".length))
      } else
        ""
    }.trim

    //remove partition key from primary key
    //todo: multiple partition key
    val primaryKeyArr = for(key <- primaryKey.split(",") if !partitionKey.equalsIgnoreCase(key.trim)) yield key.trim

    (primaryKeyArr.mkString(","), partitionKey)
  }


  def queryByJdbc(url: String, sql: String) : Seq[String] = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    var seq: Seq[String] = Seq()
    try {
      Class.forName(CLASS_NAME)
      conn = DriverManager.getConnection(url)
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery

      while (rs.next)
        seq :+= rs.getString(1)
      seq
    } catch {
      case e: Exception => e.printStackTrace
        seq
    } finally {
      if (rs != null) rs.close
      if (ps != null) ps.close
      if (conn != null) conn.close
    }

  }


  //get the table name from kafka top pattern, e.g. kudu.call_center
  def writeMultiTable2HudiFromDF(batchDF: DataFrame, database: String, writeMode: String = "upsert", zookeeperUrl: String,
                                 basePath: String, impalaJdbcUrl: String, kuduDatabase: String)(implicit spark: SparkSession) = {

    val tableList = batchDF.select("topic").distinct().rdd.map(x => x.getAs[String](0)).collect()
    if (tableList.size > 0) {
      tableList.foreach( table => {
        val tableName = table.split('.').last
//      println(LocalDateTime.now() + " ****** table name := " + tableName)

        val (primaryKey, partitionKey) = getPrimaryAndPartitionKey(impalaJdbcUrl + kuduDatabase, tableName)
//      println(LocalDateTime.now() + " ****** primary key := " + primaryKey)
//      println(LocalDateTime.now() + " ****** partition key := " + partitionKey)

        val oneBatchDF = batchDF.filter("topic = '" + table + "'")
          .withColumn("json", col("value").cast(StringType)).select("json")

        oneBatchDF.show(false)

        //val schema = getJsonSchemaFromJSONString(oneBatchDF.first.getAs[String](0))
        println(LocalDateTime.now() + s" ****** schema := $basePath/$database/$tableName")
        val schema = spark.read.format("hudi").load(s"$basePath/$database/$tableName")
          .drop(col("_hoodie_commit_seqno")).drop(col("_hoodie_commit_time"))
          .drop(col("_hoodie_record_key")).drop(col("_hoodie_partition_path"))
          .drop(col("_hoodie_file_name"))
          .schema

        println(LocalDateTime.now() + " ****** schema := " + schema.printTreeString())
        val df = oneBatchDF.select(from_json(col("json"), schema) as "data")
          .select("data.*")
          .filter(genPrimaryKeyFilter(primaryKey))

        val tableType = if (partitionKey != null && partitionKey.length > 0) "MERGE_ON_READ" else "COPY_ON_WRITE"

        writeHudiTable(df, database, tableName, writeMode, zookeeperUrl,
          primaryKey, "created_ts", partitionKey, basePath, tableType)
      })

    }
  }

  def genPrimaryKeyFilter(primaryKey: String): String = {
    val filterArr = for(key <- primaryKey.split(",")) yield key + " is not null "
    filterArr.mkString(" and ")
  }
}

