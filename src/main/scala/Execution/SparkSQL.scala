package Execution
import java.util.Properties

import org.apache.spark.sql.SparkSession
import LoadPgDBConf.ReadProperties
import Protocols._

import scala.collection.mutable.ArrayBuffer

object SparkSQL {
  val localTempView = new ArrayBuffer[String]

  def executeSqlCommand(spark: SparkSession):Unit = {

    val url = ReadProperties.getPgDB_url()
    val user = ReadProperties.getPgDB_user()
    val password = ReadProperties.getPgDB_password()

    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)

    val sqlCommand = ParseSQL.getSqlCommand()
    val inputList = ParseSQL.getInputList()
    val outputTable = ParseSQL.getOutputList()

    val numberOfTables = inputList.length

    val inputTables = new Array[String](numberOfTables)

    /*
    val jdbcDF1 = spark.read
      .jdbc(url, "student", connectionProperties)
      .createTempView("student")

    val jdbcDF2 = spark.read
      .jdbc(url, "grade", connectionProperties)
      .createTempView("grade")
    */

    for(i <- 0 until numberOfTables){
      inputTables(i) = inputList(i)
      if(!localTempView.contains(inputTables(i))){
        localTempView += inputTables(i)
        spark.read.jdbc(url, inputTables(i), connectionProperties).createTempView(inputTables(i))
      }
    }

    val executeCommand = spark.sql(sqlCommand)

    executeCommand.write
      .jdbc(url, outputTable, connectionProperties)

    println("Job is succeed...")

    //executeCommand.show()
    //executeCommand.explain()

    // Specifying create table column data types on write
    /*
    jdbcDF.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    */
  }
}
