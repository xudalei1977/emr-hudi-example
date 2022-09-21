package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

object KuduSample {

  private val log = LoggerFactory.getLogger("KuduSample")

  def main(args: Array[String]): Unit = {

    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)

    val parmas = Config.parseConfig(MSK2Hudi, args)
    val spark = SparkHelper.getSparkSession(parmas.env)

    val sfmta_raw = spark.sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("s3://dalei-demo/kudu/sfmtaAVLRawData01012013.csv")

    sfmta_raw.printSchema
    sfmta_raw.createOrReplaceTempView("sfmta_raw")
    spark.sql("SELECT count(*) FROM sfmta_raw").show()
    spark.sql("SELECT * FROM sfmta_raw LIMIT 5").show()


    //  2. Prepare the data to load into Kudu.
    //  +
    //    To preapare the data we will:
    //    +
    //  * Convert the `REPORT_TIME` from a  string into a timestamp.
    //  * Mark the primary key columns, `REPORT_TIME` and `VEHICLE_TAG`, as non-nullable.
    //    +
    //    [source,scala]
    //  ----
    //    :paste
    //  import org.apache.spark.sql.types._
    //  import org.apache.spark.sql.DataFrame
    //  def setNotNull(df: DataFrame, columns: Seq[String]) : DataFrame = {
    //    val schema = df.schema
    //    // Modify [[StructField] for the specified columns.
    //    val newSchema = StructType(schema.map {
    //      case StructField(c, t, _, m) if columns.contains(c) => StructField(c, t, nullable = false, m)
    //      case y: StructField => y
    //    })
    //    // Apply new schema to the DataFrame
    //    df.sqlContext.createDataFrame(df.rdd, newSchema)
    //  }
    //  val sftmta_time = sfmta_raw
    //    .withColumn("REPORT_TIME", to_timestamp($"REPORT_TIME", "MM/dd/yyyy HH:mm:ss"))
    //  val sftmta_prep = setNotNull(sftmta_time, Seq("REPORT_TIME", "VEHICLE_TAG"))
    //  sftmta_prep.printSchema
    //  sftmta_prep.createOrReplaceTempView("sftmta_prep")
    //  spark.sql("SELECT count(*) FROM sftmta_prep").show()
    //  spark.sql("SELECT * FROM sftmta_prep LIMIT 5").show()
    //  ----
    //
    //  == Load and prepare the Kudu table
    //
    //  1. Create a new Kudu table
    //    +
    //      Create a Kudu table with 3 replicas and 4 hash partitions using the schema
    //    defined by the sftmta_prep DataFrame.
    //    +
    //    [source,scala]
    //  ----
    //    :paste
    //  import collection.JavaConverters._
    //  import org.apache.kudu.client._
    //  import org.apache.kudu.spark.kudu._
    //  val kuduContext = new KuduContext("localhost:7051,localhost:7151,localhost:7251", spark.sparkContext)
    //
    //  // Delete the table if it already exists.
    //  if(kuduContext.tableExists("sfmta_kudu")) {
    //    kuduContext.deleteTable("sfmta_kudu")
    //  }
    //
    //  kuduContext.createTable("sfmta_kudu", sftmta_prep.schema,
    //    /* primary key */ Seq("REPORT_TIME", "VEHICLE_TAG"),
    //    new CreateTableOptions()
    //      .setNumReplicas(3)
    //      .addHashPartitions(List("VEHICLE_TAG").asJava, 4))
    //  ----
    //  +
    //    NOTE: The table is deleted and recreated if it already exists.
    //
    //  2. Load the Kudu table.
    //    +
    //  Insert the prepared data into the Kudu table using the `kuduContext`.
    //    +
    //    [source,scala]
    //  ----
    //    :paste
    //  kuduContext.insertRows(sftmta_prep, "sfmta_kudu")
    //  // Create a DataFrame that points to the Kudu table we want to query.
    //  val sfmta_kudu = spark.read
    //    .option("kudu.master", "localhost:7051,localhost:7151,localhost:7251")
    //    .option("kudu.table", "sfmta_kudu")
    //    // We need to use leader_only because Kudu on Docker currently doesn't
    //    // support Snapshot scans due to `--use_hybrid_clock=false`.
    //    .option("kudu.scanLocality", "leader_only")
    //    .format("kudu").load
    //  sfmta_kudu.createOrReplaceTempView("sfmta_kudu")
    //  spark.sql("SELECT count(*) FROM sfmta_kudu").show()
    //  spark.sql("SELECT * FROM sfmta_kudu LIMIT 5").show()
    //  ----
    //
    //  == Read and Modify Data
    //
    //  Now that the data is stored in Kudu, you can run queries against it.
    //  The following query finds the data point containing the highest recorded vehicle speed.
    //
    //    [source,scala]
    //  ----
    //  spark.sql("SELECT * FROM sfmta_kudu ORDER BY speed DESC LIMIT 1").show()
    //  ----
    //
    //  The output should look something like this:
    //
    //  [source,scala]
    //  ----
    //  +-------------+-------------+--------------------+-------------------+-------------------+---------+
    //  | report_time | vehicle_tag | longitude          | latitude          | speed             | heading |
    //  +-------------+-------------+--------------------+-------------------+-------------------+---------+
    //  | 1357022342  | 5411        | -122.3968811035156 | 37.76665878295898 | 68.33300018310547 | 82      |
    //    +-------------+-------------+--------------------+-------------------+-------------------+---------+
    //  ----
    //
    //  With a quick link:https://www.google.com/search?q=122.3968811035156W+37.76665878295898N[Google search]
    //  we can see that this bus was traveling east on 16th street at 68MPH.
    //  At first glance, this seems unlikely to be true. Perhaps we do some research
    //    and find that this bus's sensor equipment was broken and we decide to
    //  remove the data. With Kudu this is very easy to correct using Spark:
    //
    //  [source,scala]
    //  ----
    //  spark.sql("SELECT count(*) FROM sfmta_kudu WHERE vehicle_tag = 5411").show()
    //  val toDelete = spark.sql("SELECT * FROM sfmta_kudu WHERE vehicle_tag = 5411")
    //  kuduContext.deleteRows(toDelete, "sfmta_kudu")
    //  spark.sql("SELECT count(*) FROM sfmta_kudu WHERE vehicle_tag = 5411").show()
  }
}
