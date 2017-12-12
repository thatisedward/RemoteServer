package Execution
import java.util.Properties

import org.apache.spark.sql.SparkSession
import LoadPgDBConf.ReadProperties
import Protocols._
import org.zeromq.ZMQ.Socket

import scala.collection.mutable.ArrayBuffer

object SparkSQL {
  val localTempView = new ArrayBuffer[String]

  def executeSqlCommand(spark: SparkSession, socket: Socket ):Unit = {

    val url = ReadProperties.getPgDB_url()
    val user = ReadProperties.getPgDB_user()
    val password = ReadProperties.getPgDB_password()

    val connectionProperties = new Properties()

    try{
      connectionProperties.put("user", user)
      connectionProperties.put("password", password)
    }catch{
      case e: InterruptedException => e.printStackTrace()
    }

    val sqlCommand = ParseSQL.getSqlCommand()
    val inputList = ParseSQL.getInputList()
    val outputTable = ParseSQL.getOutputList()

    val numberOfTables = inputList.length

    val inputTables = new Array[String](numberOfTables)

    /*
    val jdbcDF = spark.read
      .jdbc(url, "table_name", connectionProperties)
      .createTempView("tempView_name")
    */

    for(i <- 0 until numberOfTables) {
      inputTables(i) = inputList(i)

      if (!localTempView.contains(inputTables(i))) {
        localTempView += inputTables(i)

        spark.read
          .jdbc(url, inputTables(i), connectionProperties)
          .createTempView(inputTables(i))
      }
    }

    try {

      val executeCommand = spark.sql(sqlCommand)
      println("The SQL command is executed.")

      executeCommand.write
        .jdbc(url, outputTable, connectionProperties)

      println("SQL Job " + ParseSQL.getJobNo() + " is succeeded...")

      val reply = ("SQL Job " + ParseSQL.getJobNo() + " is succeeded...").getBytes
      reply(reply.length-1)=0
      socket.send(reply, 0)

      executeCommand.show()
    }
    catch {
        case e: Exception => {
          e.printStackTrace()

          val exception = e.toString.getBytes()
          exception(exception.length - 1) = 0
          socket.send(exception, 0)

          /*
          spark.stop()
          println("Stop spark session...")
          spark.close()
          println("Close spark session...")
          */
        }
    }
  }
    //executeCommand.explain()

    // Specifying create table column data types on write
    /*
    jdbcDF.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    */
}
