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

        val df = spark.read
          .jdbc(url, inputTables(i), connectionProperties)

        df.createOrReplaceTempView(inputTables(i))

        df.cache()
      }

    }

    try {

      val sqlExecuteTime_start = System.currentTimeMillis()

      val ec = spark.sql(sqlCommand)

      ec.cache()

      val sqlExecuteTime = System.currentTimeMillis()-sqlExecuteTime_start

      println("=> The SQL command was executed in: "+ sqlExecuteTime+ " ms...")

      val JDBCWriteBackTime_start = System.currentTimeMillis()

      ec.write
        .jdbc(url, outputTable, connectionProperties)

      val JDBCWriteBackTime = System.currentTimeMillis()-JDBCWriteBackTime_start

      val printInfo = "\nSQL Job " + ParseSQL.getJobNo() + " is completed.\n" +
        "=> The SQL command was executed: "+ sqlExecuteTime+ " ms.\n"+
        "=> Write back to PostgresSQL was finished: "+ JDBCWriteBackTime + " ms.\n"

      println(printInfo)

      val reply = (printInfo).getBytes
      reply(reply.length-1)=0
      socket.send(reply, 0)

    }
    catch {
        case e: Exception => {
          e.printStackTrace()
          val notifyMessage = "\nSQL Job " + ParseSQL.getJobNo() + " failed, causing by:\n"
          val exception = (notifyMessage + e.toString).getBytes()

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
