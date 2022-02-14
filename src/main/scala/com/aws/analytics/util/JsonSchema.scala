package com.aws.analytics.util

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object JsonSchema {

  //private val log = LoggerFactory.getLogger("JsonSchema")

  def getJsonSchemaFromJSONString(jsonString: String): StructType= {

    //json sample
//    val jsonString = """{"id":93583,"status":3,"age":97,"phone":15879193008,"email":"jiehan@yahoo.com","ip":"198.17.166.99","cardDate":"11/31","creditCardNumber":"374210163782579","startAddress":"陕西省广州县吉区哈尔滨街h座 107982",
//                       |"endAddress":"贵州省平市长寿张路A座 940371","carNumber":"0-496-12860-4","carType":"a","userName":"匡建国","userID":"610327194507195299","driverName":"曾琴","driverRegisterDate":"20201011","score":"82.35",
//                       |"startLatitude":"43.9439010","startLongitude":"14.1062869","endLatitude":"96.5339511","endLongitude":"80.2033000","money":"58.49","createTS":1644645838,"eventTS":1644645841,"__op":"u","__source_connector":"mysql",
//                       |"__source_db":"tesla","__source_table":"taxi_order","__source_file":"mysql-bin-changelog.000003","__source_pos":45246788,"__source_ts_ms":1644645841000,"__deleted":"false"}""".stripMargin

    val jsonParser = new JSONParser(1)
    val jsonObj: JSONObject = jsonParser.parse(jsonString).asInstanceOf[JSONObject]

    getJsonSchemaFromJSONObject(jsonObj)
  }


  def getJsonSchemaFromJSONObject(jsonObj: JSONObject): StructType= {

    val jsonKey = jsonObj.keySet()
    var iter = jsonKey.iterator

    var structFieldSeq: Seq[StructField] = Seq()
    while (iter.hasNext) {
      val key = iter.next()
      val value = jsonObj.get(key)

      value.getClass.getSimpleName match {
        case "Double" => structFieldSeq :+= StructField(key, DoubleType, true)
        case "Integer" => structFieldSeq :+= StructField(key, IntegerType, true)
        case "Long" => structFieldSeq :+= StructField(key, LongType, true)
        case "String" => structFieldSeq :+= StructField(key, StringType, true)
        case _ => structFieldSeq :+= StructField(key, StringType, true)
      }
    }
    StructType(structFieldSeq)
  }

  def getValueFromJSONString(jsonString: String, key: String): String= {

    val jsonParser = new JSONParser(1)
    val jsonObj: JSONObject = jsonParser.parse(jsonString).asInstanceOf[JSONObject]

    if(jsonObj.get(key) == null)
      "None"
    else
      jsonObj.get(key).toString
  }

}
