package com.aws.analytics.conf

import com.aws.analytics.cdc.util.JsonUtil

case class Config(
                   env: String = "",
                   brokerList: String = "",
                   sourceTopic: String = "",
                   consumerGroup: String = "",
                   checkpointDir: String ="",
                   path:String = "",
                   tableInfoJson: String ="",
                   trigger:String = "300",
                   hudiEventBasePath:String ="",
                   tableType:String="COW",
                   syncDB:String ="",
                   syncTableName:String ="",
                   syncJDBCUrl:String ="",
                   syncJDBCUsername:String ="hive" ,
                   jsonMetaSample:String ="",
                   syncHive:String="false",
                   partitionNum:String ="16",
                   partitionFormat:String ="yyyy-MM-dd-HH-mm",
                   startPos:String="latest",
                   morCompact:String="true",
                   inlineMax:String="20",
                   hudiWriteOperation:String = "insert" ,
                   hudiKeyField:String = "",
                   hudiPartition:String = "logday,hm",
                   concurrent:String = "false",
                   zookeeperUrl:String = "",
                   zookeeperPort:String = "2181",
                   hudiAggBasePath:String = ""
                 )


object Config {

  def parseConfig(obj: Object,args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[Config]("spark ss "+programName) {
      head(programName, "1.0")
      opt[String]('e', "env").required().action((x, config) => config.copy(env = x)).text("env: dev or prod")
      opt[String]('b', "brokerList").optional().action((x, config) => config.copy(brokerList = x)).text("kafka broker list,sep comma")
      opt[String]('t', "sourceTopic").optional().action((x, config) => config.copy(sourceTopic = x)).text("kafka topic")
      opt[String]('p', "consumerGroup").optional().action((x, config) => config.copy(consumerGroup = x)).text("kafka consumer group")
      opt[String]('s', "syncHive").optional().action((x, config) => config.copy(syncHive = x)).text("whether sync hive，default:false")
      opt[String]('o', "startPos").optional().action((x, config) => config.copy(startPos = x)).text("kafka start pos latest or earliest,default latest")
      opt[String]('i', "trigger").optional().action((x, config) => config.copy(trigger = x)).text("default 300 second,streaming trigger interval")
      opt[String]('c', "checkpointDir").optional().action((x, config) => config.copy(checkpointDir = x)).text("hdfs dir which used to save checkpoint")

      programName match {
        case "Canal2Hudi" =>
          opt[String]('m', "tableInfoJson").optional().action((x, config) => config.copy(tableInfoJson =x)).text("table info json str")
          opt[String]('g', "hudiEventBasePath").required().action((x, config) => config.copy(hudiEventBasePath = x)).text("hudi event table hdfs base path")
          opt[String]('y', "tableType").optional().action((x, config) => config.copy(tableType = x)).text("hudi table type MOR or COW. default COW")
          opt[String]('t', "morCompact").optional().action((x, config) => config.copy(morCompact = x)).text("mor inline compact,default:true")
          opt[String]('m', "inlineMax").optional().action((x, config) => config.copy(inlineMax = x)).text("inline max compact,default:20")
          opt[String]('r', "syncJDBCUrl").optional().action((x, config) => config.copy(syncJDBCUrl = x)).text("hive server2 jdbc, eg. jdbc:hive2://172.17.106.165:10000")
          opt[String]('n', "syncJDBCUsername").optional().action((x, config) => config.copy(syncJDBCUsername = x)).text("hive server2 jdbc username, default: hive")
          opt[String]('p', "partitionNum").optional().action((x, config) => config.copy(partitionNum = x)).text("repartition num,default 16")
          opt[String]('w', "hudiWriteOperation").optional().action((x, config) => config.copy(hudiWriteOperation = x)).text("hudi write operation,default insert")
          opt[String]('u', "concurrent").optional().action((x, config) => config.copy(concurrent = x)).text("write multiple hudi table concurrent,default false")

        case "Log2Hudi" =>
          opt[String]('j', "jsonMetaSample").required().action((x, config) => config.copy(jsonMetaSample = x)).text("json meta sample")
          opt[String]('g', "hudiEventBasePath").required().action((x, config) => config.copy(hudiEventBasePath = x)).text("hudi event table hdfs base path")
          opt[String]('s', "syncDB").optional().action((x, config) => config.copy(syncDB = x)).text("hudi sync hive db")
          opt[String]('u', "syncTableName").optional().action((x, config) => config.copy(syncTableName = x)).text("hudi sync hive table name")
          opt[String]('y', "tableType").optional().action((x, config) => config.copy(tableType = x)).text("hudi table type MOR or COW. default COW")
          opt[String]('r', "syncJDBCUrl").optional().action((x, config) => config.copy(syncJDBCUrl = x)).text("hive server2 jdbc, eg. jdbc:hive2://172.17.106.165:10000")
          opt[String]('n', "syncJDBCUsername").optional().action((x, config) => config.copy(syncJDBCUsername = x)).text("hive server2 jdbc username, default: hive")
          opt[String]('p', "partitionNum").optional().action((x, config) => config.copy(partitionNum = x)).text("repartition num,default 16")
          opt[String]('t', "morCompact").optional().action((x, config) => config.copy(morCompact = x)).text("mor inline compact,default:true")
          opt[String]('m', "inlineMax").optional().action((x, config) => config.copy(inlineMax = x)).text("inline max compact,default:20")
          opt[String]('w', "hudiWriteOperation").optional().action((x, config) => config.copy(hudiWriteOperation = x)).text("hudi write operation,default insert")
          opt[String]('z', "hudiKeyField").required().action((x, config) => config.copy(hudiKeyField = x)).text("hudi key field, recordkey,precombine")
          opt[String]('q', "hudiPartition").required().action((x, config) => config.copy(hudiPartition = x)).text("hudi partition column,default logday,hm")

        case "MSK2Hudi" =>
          opt[String]('y', "tableType").optional().action((x, config) => config.copy(tableType = x)).text("hudi table type MOR or COW. default COW")
          opt[String]('t', "morCompact").optional().action((x, config) => config.copy(morCompact = x)).text("mor inline compact,default:true")
          opt[String]('m', "inlineMax").optional().action((x, config) => config.copy(inlineMax = x)).text("inline max compact,default:20")
          opt[String]('s', "syncDB").optional().action((x, config) => config.copy(syncDB = x)).text("hudi sync hive db")
          opt[String]('u', "syncTableName").required().action((x, config) => config.copy(syncTableName = x)).text("hudi sync hive table name")
          opt[String]('w', "hudiWriteOperation").optional().action((x, config) => config.copy(hudiWriteOperation = x)).text("hudi write operation,default insert")
          opt[String]('z', "hudiKeyField").required().action((x, config) => config.copy(hudiKeyField = x)).text("hudi key field, recordkey,precombine")
          opt[String]('q', "hudiPartition").optional().action((x, config) => config.copy(hudiPartition = x)).text("hudi partition column,default logday,hm")
          opt[String]('g', "hudiEventBasePath").required().action((x, config) => config.copy(hudiEventBasePath = x)).text("hudi event table hdfs base path")
          opt[String]('l', "zookeeperUrl").optional().action((x, config) => config.copy(zookeeperUrl = x)).text("zookeeper url for hudi")
          opt[String]('t', "zookeeperPort").optional().action((x, config) => config.copy(zookeeperPort = x)).text("zookeeper port for hudi")

        case "Aggregation" =>
          opt[String]('g', "hudiEventBasePath").required().action((x, config) => config.copy(hudiEventBasePath = x)).text("hudi event table hdfs base path")
          opt[String]('a', "hudiAggBasePath").optional().action((x, config) => config.copy(hudiAggBasePath = x)).text("hudi aggregation table hdfs base path")

        case "Hudi2MSK" =>
          opt[String]('w', "hudiWriteOperation").optional().action((x, config) => config.copy(hudiWriteOperation = x)).text("hudi write operation,default insert")
          opt[String]('j', "jsonMetaSample").required().action((x, config) => config.copy(jsonMetaSample = x)).text("json meta sample")
          opt[String]('g', "hudiEventBasePath").required().action((x, config) => config.copy(hudiEventBasePath = x)).text("hudi event table hdfs base path")
          opt[String]('s', "syncDB").optional().action((x, config) => config.copy(syncDB = x)).text("hudi sync hive db")
          opt[String]('u', "syncTableName").optional().action((x, config) => config.copy(syncTableName = x)).text("hudi sync hive table name")
          opt[String]('y', "tableType").optional().action((x, config) => config.copy(tableType = x)).text("hudi table type MOR or COW. default COW")
          opt[String]('r', "syncJDBCUrl").optional().action((x, config) => config.copy(syncJDBCUrl = x)).text("hive server2 jdbc, eg. jdbc:hive2://172.17.106.165:10000")
          opt[String]('n', "syncJDBCUsername").optional().action((x, config) => config.copy(syncJDBCUsername = x)).text("hive server2 jdbc username, default: hive")
          opt[String]('p', "partitionNum").optional().action((x, config) => config.copy(partitionNum = x)).text("repartition num,default 16")
          opt[String]('t', "morCompact").optional().action((x, config) => config.copy(morCompact = x)).text("mor inline compact,default:true")
          opt[String]('m', "inlineMax").optional().action((x, config) => config.copy(inlineMax = x)).text("inline max compact,default:20")
          opt[String]('z', "hudiKeyField").required().action((x, config) => config.copy(hudiKeyField = x)).text("hudi key field, recordkey,precombine")
          opt[String]('q', "hudiPartition").required().action((x, config) => config.copy(hudiPartition = x)).text("hudi partition column,default logday,hm")
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

