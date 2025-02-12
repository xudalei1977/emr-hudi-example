package com.aws.analytics.conf

case class Config(
                   env: String = "",
                   brokerList: String = "",
                   sourceTopic: String = "",
                   consumerGroup: String = "",
                   checkpointDir: String ="",
                   path:String = "",
                   tableInfoJson: String ="",
                   trigger:String = "300",
                   hudiBasePath:String ="",
                   tableType:String="COW",
                   syncDB:String ="",
                   syncTableName:String ="",
                   syncJDBCUrl:String ="",
                   syncJDBCUsername:String ="hive" ,
                   jsonMetaSample:String ="",
                   syncHive:String="false",
                   partitionNum:Int=16,
                   partitionFormat:String ="yyyy-MM-dd-HH-mm",
                   startPos:String="latest",
                   morCompact:String="true",
                   inlineMax:String="20",
                   hudiWriteOperation:String = "insert" ,
                   hudiKeyField:String = "",
                   hudiCombineField:String = "",
                   hudiPartition:String = "",
                   concurrent:String = "false",
                   zookeeperUrl:String = "",
                   zookeeperPort:String = "2181",
                   hudiAggBasePath:String = "",
                   rdsHost:String = "",
                   rdsDatabase:String = "",
                   rdsTableName:String = "",
                   rdsUserName:String = "",
                   rdsPassword:String = "",
                   rdsDataInS3:String = "",
                   sourceDateRange: String = "",
                   hiveHost: String = "",
                   hiveDatabase: String = "",
                   hivePartitionValue: String = "",
                   hiveTablePath: String = "",
                   hudiIntervel: Int = 30000,
                   hiveFilter: String = "",
                   auditResult: String = ""
                 )

object Config {

  def parseConfig(obj: Object,args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[Config]("spark ss "+programName) {
      head(programName, "1.0")
      opt[String]('e', "env").required().action((x, config) => config.copy(env = x)).text("env: dev or prod")

      programName match {
        case "Hive2Hudi" =>
          opt[String]('P', "hiveTablePath").optional().action((x, config) => config.copy(hiveTablePath = x)).text("hive table path")
          opt[String]('c', "checkpointDir").optional().action((x, config) => config.copy(checkpointDir = x)).text("hdfs dir which used to save checkpoint")
          opt[String]('g', "hudiBasePath").optional().action((x, config) => config.copy(hudiBasePath = x)).text("hudi event table hdfs base path")
          opt[String]('y', "tableType").optional().action((x, config) => config.copy(tableType = x)).text("hudi table type MOR or COW. default COW")
          opt[String]('s', "syncDB").optional().action((x, config) => config.copy(syncDB = x)).text("hudi sync hive db")
          opt[String]('u', "syncTableName").optional().action((x, config) => config.copy(syncTableName = x)).text("hudi sync hive table name")
          opt[String]('w', "hudiWriteOperation").optional().action((x, config) => config.copy(hudiWriteOperation = x)).text("hudi write operation,default insert")
          opt[String]('z', "hudiKeyField").optional().action((x, config) => config.copy(hudiKeyField = x)).text("hudi key field, recordkey")
          opt[String]('m', "hudiCombineField").optional().action((x, config) => config.copy(hudiCombineField = x)).text("hudi combine field, precombine")
          opt[String]('q', "hudiPartition").optional().action((x, config) => config.copy(hudiPartition = x)).text("hudi partition column,default logday,hm")
          opt[String]('t', "morCompact").optional().action((x, config) => config.copy(morCompact = x)).text("mor inline compact,default:true")
          opt[String]('m', "inlineMax").optional().action((x, config) => config.copy(inlineMax = x)).text("inline max compact,default:20")
          opt[String]('l', "zookeeperUrl").optional().action((x, config) => config.copy(zookeeperUrl = x)).text("zookeeper url for hudi")

        case "MSK2Hudi" =>
          opt[String]('b', "brokerList").optional().action((x, config) => config.copy(brokerList = x)).text("kafka broker list,sep comma")
          opt[String]('t', "sourceTopic").optional().action((x, config) => config.copy(sourceTopic = x)).text("kafka topic")
          opt[String]('p', "consumerGroup").optional().action((x, config) => config.copy(consumerGroup = x)).text("kafka consumer group")
          opt[String]('o', "startPos").optional().action((x, config) => config.copy(startPos = x)).text("kafka start pos latest or earliest,default latest")
          opt[String]('i', "trigger").optional().action((x, config) => config.copy(trigger = x)).text("default 300 second,streaming trigger interval")
          opt[String]('c', "checkpointDir").optional().action((x, config) => config.copy(checkpointDir = x)).text("hdfs dir which used to save checkpoint")
          opt[String]('g', "hudiBasePath").optional().action((x, config) => config.copy(hudiBasePath = x)).text("hudi event table hdfs base path")
          opt[String]('y', "tableType").optional().action((x, config) => config.copy(tableType = x)).text("hudi table type MOR or COW. default COW")
          opt[String]('s', "syncDB").optional().action((x, config) => config.copy(syncDB = x)).text("hudi sync hive db")
          opt[String]('u', "syncTableName").optional().action((x, config) => config.copy(syncTableName = x)).text("hudi sync hive table name")
          opt[String]('w', "hudiWriteOperation").optional().action((x, config) => config.copy(hudiWriteOperation = x)).text("hudi write operation,default insert")
          opt[String]('z', "hudiKeyField").optional().action((x, config) => config.copy(hudiKeyField = x)).text("hudi key field, recordkey")
          opt[String]('m', "hudiCombineField").optional().action((x, config) => config.copy(hudiCombineField = x)).text("hudi combine field, precombine")
          opt[String]('q', "hudiPartition").optional().action((x, config) => config.copy(hudiPartition = x)).text("hudi partition column,default logday,hm")
          opt[String]('t', "morCompact").optional().action((x, config) => config.copy(morCompact = x)).text("mor inline compact,default:true")
          opt[String]('m', "inlineMax").optional().action((x, config) => config.copy(inlineMax = x)).text("inline max compact,default:20")
          opt[String]('l', "zookeeperUrl").optional().action((x, config) => config.copy(zookeeperUrl = x)).text("zookeeper url for hudi")

        case "ODS2DWD" =>
          opt[Int]('i', "hudiIntervel").optional().action((x, config) => config.copy(hudiIntervel = x)).text("interval for hudi incremetail query")

        case "DWD2DM" =>
          opt[Int]('i', "hudiIntervel").optional().action((x, config) => config.copy(hudiIntervel = x)).text("interval for hudi incremetail query")

        case "RDS2S3" =>
          opt[String]('h', "rdsHost").optional().action((x, config) => config.copy(rdsHost = x)).text("rdsHost")
          opt[String]('d', "rdsDatabase").optional().action((x, config) => config.copy(rdsDatabase = x)).text("rdsDatabase")
          opt[String]('t', "rdsTableName").optional().action((x, config) => config.copy(rdsTableName = x)).text("rdsTableName")
          opt[String]('u', "rdsUserName").optional().action((x, config) => config.copy(rdsUserName = x)).text("rdsUserName")
          opt[String]('p', "rdsPassword").optional().action((x, config) => config.copy(rdsPassword = x)).text("rdsPassword")
          opt[String]('s', "rdsDataInS3").optional().action((x, config) => config.copy(rdsDataInS3 = x)).text("rdsDataInS3")
          opt[String]('r', "sourceDateRange").optional().action((x, config) => config.copy(sourceDateRange = x)).text("sourceDateRange")
          opt[Int]('n', "partitionNum").optional().action((x, config) => config.copy(partitionNum = x)).text("partitionNum")
          opt[String]('H', "hiveHost").optional().action((x, config) => config.copy(hiveHost = x)).text("hiveHost")
          opt[String]('D', "hiveDatabase").optional().action((x, config) => config.copy(hiveDatabase = x)).text("hiveDatabase")
          opt[String]('P', "hivePartitionValue").optional().action((x, config) => config.copy(hivePartitionValue = x)).text("hivePartitionValue")
          opt[String]('S', "hiveTablePath").optional().action((x, config) => config.copy(hiveTablePath = x)).text("hive table path")

        case "DataAudit" =>
          opt[String]('H', "hiveHost").optional().action((x, config) => config.copy(hiveHost = x)).text("hiveHost")
          opt[String]('D', "hiveDatabase").optional().action((x, config) => config.copy(hiveDatabase = x)).text("hiveDatabase")
          opt[String]('F', "hiveFilter").optional().action((x, config) => config.copy(hiveFilter = x)).text("hiveFilter")
          opt[String]('R', "auditResult").optional().action((x, config) => config.copy(auditResult = x)).text("auditResult")
      }

    }

    parser.parse(args, Config()) match {
      case Some(conf) => conf
      case None => {
        System.exit(-1)
        null
      }
    }

  }

}


