package com.aws.analytics.util

import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties
import scala.collection.immutable.{IndexedSeq, Seq, Set}
import com.aws.analytics.conf.Config
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import scala.util.{Try, Using}

class MySQLUtil {
    private val logger: Logger = LoggerFactory.getLogger("MySQLUtil")
    private val CLASS_NAME = "com.mysql.cj.jdbc.Driver"

    // Connection pool configuration
    private var dataSource: HikariDataSource = _

    // Initialize connection pool
    private def initializeConnectionPool(conf: Config): Unit = {
        if (dataSource == null) {
            val hikariConfig = new HikariConfig()
            hikariConfig.setJdbcUrl(getJDBCUrl(conf))
            hikariConfig.setUsername(conf.rdsUserName)
            hikariConfig.setPassword(conf.rdsPassword)
            hikariConfig.setMaximumPoolSize(10)
            hikariConfig.setMinimumIdle(5)
            hikariConfig.setIdleTimeout(300000)
            hikariConfig.setConnectionTimeout(20000)

            dataSource = new HikariDataSource(hikariConfig)
        }
    }

    def getJDBCUrl(conf: Config): String = {
        val baseUrl = s"jdbc:mysql://${conf.rdsHost}:3306/${conf.rdsDatabase}"
        val params = Seq(
            "useSSL=false",
            "tinyInt1isBit=false",
            "rewriteBatchedStatements=true",
            "useUnicode=true",
            "characterEncoding=UTF-8",
            "serverTimezone=UTC"
        ).mkString("&")

        s"$baseUrl?$params"
    }

    def queryByJdbc(conf: Config, sql: String): Seq[String] = {
        initializeConnectionPool(conf)

        Using.Manager { use =>
            val conn = use(dataSource.getConnection)
            val ps = use(conn.prepareStatement(sql))
            val rs = use(ps.executeQuery())

            val builder = Seq.newBuilder[String]
            while (rs.next) {
                builder += rs.getString(1)
            }
            builder.result()
        }.getOrElse {
            logger.error(s"Failed to execute query: $sql")
            Seq.empty
        }
    }

    def getConnection(conf: Config): Connection = {
        initializeConnectionPool(conf)
        Try {
            dataSource.getConnection
        }.getOrElse {
            logger.error("Failed to get connection from pool, falling back to direct connection")
            createDirectConnection(conf)
        }
    }

    private def createDirectConnection(conf: Config): Connection = {
        val connectionProps = new Properties()
        connectionProps.put("user", conf.rdsUserName)
        connectionProps.put("password", conf.rdsPassword)

        Try {
            Class.forName(CLASS_NAME)
            DriverManager.getConnection(getJDBCUrl(conf), connectionProps)
        }.recover { case e =>
            logger.error(s"Failed to create direct connection: ${e.getMessage}")
            throw e
        }.get
    }

    // Resource cleanup
    def closePool(): Unit = {
        Option(dataSource).foreach { ds =>
            ds.close()
            dataSource = null
        }
    }

    // Batch operations helper
    def executeBatch[T](conf: Config, sql: String, params: Seq[T])(setParams: (PreparedStatement, T) => Unit): Int = {
        Using.Manager { use =>
            val conn = use(getConnection(conf))
            val ps = use(conn.prepareStatement(sql))

            params.foreach { param =>
                setParams(ps, param)
                ps.addBatch()
            }

            ps.executeBatch().sum
        }.getOrElse {
            logger.error(s"Failed to execute batch operation")
            0
        }
    }

    // Helper method for prepared statements with automatic resource management
    def executeQuery[T](conf: Config, sql: String)(process: ResultSet => T): Option[T] = {
        Using.Manager { use =>
            val conn = use(getConnection(conf))
            val ps = use(conn.prepareStatement(sql))
            val rs = use(ps.executeQuery())

            Option(process(rs))
        }.toOption.flatten
    }

    //Use this method to get the columns to extract
    def getValidFieldNames(conf: Config, tableName: String): String = {
        val tableDetails = getTableDetails(conf, tableName)
        tableDetails.validFields.map(r => s""" ${r.fieldName.toLowerCase} """).mkString(",")
    }

    def getTableDetails(conf: Config, tableName: String): TableDetails = {
        logger.info(s"************************ tableName: ${tableName}")
        val conn = getConnection(conf)
        val stmt = conn.createStatement()
        val query = s"SELECT * from `${conf.rdsDatabase}`.`${tableName}` where 1 < 0"
        val rs = stmt.executeQuery(query)
        val rsmd = rs.getMetaData
        val validFieldTypes = mysqlToHiveTypeConverter.keys.toSet
        var validFields = Seq[DBField]()

        var setColumns = scala.collection.immutable.Set[String]()
        logger.info(s"************************ getColumnCount: ${rsmd.getColumnCount}")
        for (i <- 1 to rsmd.getColumnCount) {
            val columnType = rsmd.getColumnTypeName(i)

            if (validFieldTypes.contains(columnType.toUpperCase)) {
                val hiveColumnType = convertMySqlTypeToHiveType(columnType)
                val javaTypeMapping = {
                    if (hiveColumnType == "TIMESTAMP" || hiveColumnType == "DATE") Some("String")
                    else None
                }
                validFields = validFields :+ DBField(rsmd.getColumnName(i), hiveColumnType, javaTypeMapping)
            } else {
                validFields = validFields :+ DBField(rsmd.getColumnName(i), columnType, None)
            }
            setColumns = setColumns + rsmd.getColumnName(i).toLowerCase
            logger.info(s" column: ${rsmd.getColumnName(i)}, type: ${rsmd.getColumnTypeName(i)}," +
              s" precision: ${rsmd.getPrecision(i)}, scale:${rsmd.getScale(i)}\n")
        }
        rs.close()
        stmt.close()
//        val primaryKey = getPrimaryKey(conn, tableName, setColumns, conf)
//        val recordStat =
//            if (primaryKey != None && (! primaryKey.getOrElse("").contains(",")))
//                getRecordStat(conn, tableName, primaryKey.get, conf)
//            else
//                ("0", "0", "0")
        conn.close()
        TableDetails(validFields, null, null, null, null, null)
    }

    def getPrimaryKey(conn: Connection, tableName: String, setColumns: Set[String], conf: Config): Option[String] = {
        val meta = conn.getMetaData
        val resPrimaryKeys = meta.getPrimaryKeys(conf.rdsDatabase, null, tableName)
        var primaryKeys = scala.collection.immutable.Set[String]()

        while (resPrimaryKeys.next) {
            val columnName = resPrimaryKeys.getString(4)
            if (setColumns.contains(columnName.toLowerCase)) {
                primaryKeys = primaryKeys + columnName
            } else {
                logger.warn(s"Could not access primary key $columnName")
            }
        }

        resPrimaryKeys.close()

        if (primaryKeys.size > 1) {
            Some(primaryKeys.mkString(","))
        } else if (primaryKeys.size == 1)  {
            Some(primaryKeys.toSeq.head)
        } else {
            None
        }
    }

    def getRecordStat(conn: Connection, tableName: String, primaryKey: String, conf: Config): (String, String, String) = {
        try {
            val stmt = conn.createStatement()
            var sql = s"select min(${primaryKey}), max(${primaryKey}), count(${primaryKey}) from `${conf.rdsDatabase}`.`${tableName}`"
            if (conf.sourceDateRange != "")
                sql = sql + s" where date_format(last_update_time, '%Y-%m-%d') in (${conf.sourceDateRange})"

            logger.warn(s"****************** the sql is : $sql")

            val rs = stmt.executeQuery(sql)
            (rs.getString(1), rs.getString(2), rs.getString(3))
        } catch {
            case e: Exception => e.printStackTrace
                ("0", "0", "0")
        }
    }

    def convertMySqlTypeToHiveType(columnType: String): String = {
        var hiveType: String = mysqlToHiveTypeConverter(columnType.toUpperCase)
        if (hiveType == null)
            hiveType = columnType.toUpperCase

        hiveType
    }

    val mysqlToHiveTypeConverter: Map[String, String] = {
        val maxVarcharSize = 65535
        Map(
            "TIME" -> "STRING",
            "DATETIME" -> "TIMESTAMP",
            "YEAR" -> "INT",
            "CHAR" -> "STRING",
            "VARCHAR" -> "STRING",
            "TINYBLOB" -> "BINARY",
            "BLOB" -> "BINARY",
            "MEDIUMBLOB" -> "BINARY",
            "LONGBLOB" -> "BINARY",
            "TINYTEXT" -> "STRING",
            "TEXT" -> "STRING",
            "JSON" -> "STRING",
            "MEDIUMTEXT" -> "STRING",
            "LONGTEXT" -> "STRING",
            "ENUM" -> "STRING",
            "SET" -> "STRING",
            "DECIMAL" -> "DECIMAL(38,10)",
            "TINYINT UNSIGNED" -> "SMALLINT",
            "SMALLINT UNSIGNED" -> "INT",
            "MEDIUMINT UNSIGNED" -> "INT",
            "INT UNSIGNED" -> "BIGINT",
            "BIGINT UNSIGNED" -> "BIGINT"
        )
    }
}

case class DBField(fieldName: String,
                   fieldType: String,
                   javaType: Option[String] = None) {

    override def toString: String = {
        s"""{
           |   Field Name: $fieldName,
           |   Field Type: $fieldType,
           |   Java Type: $javaType
           |}""".stripMargin
    }

    override def equals(obj: Any): Boolean = {
        obj match {
            case d: DBField => d.fieldName == fieldName
            case _ => false
        }
    }

    override def hashCode(): Int = {
        val prime = 31
        var result = 1
        result = prime * result + fieldName.hashCode()
        result
    }
}

case class TableDetails(validFields: Seq[DBField],
                        invalidFields: Seq[DBField],
                        sortKeys: Seq[String],
                        distributionKey: Option[String],
                        primaryKey: Option[String],
                        recordStat: (String, String, String)) {

    override def toString: String = {
        s"""{
           |   Valid Fields: $validFields,
           |   Invalid Fields: $invalidFields,
           |   Interleaved Sort Keys: $sortKeys,
           |   Distribution Keys: $distributionKey,
           |   Primary Keys: $primaryKey
           |}""".stripMargin
    }
}



// Companion object for singleton instance
object MySQLUtil {
    private val instance = new MySQLUtil()

    def getInstance: MySQLUtil = instance

    // Ensure cleanup on JVM shutdown
    Runtime.getRuntime.addShutdownHook(new Thread(() => instance.closePool()))
}
