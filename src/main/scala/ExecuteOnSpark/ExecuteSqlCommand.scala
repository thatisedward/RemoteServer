package ExecuteOnSpark

import java.util.Properties

import org.apache.spark.sql.SparkSession
import LoadPgDBConf.ReadProperties
import Protocols.ParseUDFCommand

import scala.collection.mutable.ArrayBuffer

object ExecuteSqlCommand {

  val localTempView = new ArrayBuffer[String]

  def executeSqlCommand(spark: SparkSession):Unit = {

    //val urlLocal = "jdbc:postgresql://localhost/test_db"
    val url = ReadProperties.getPgDB_url()
    val user = ReadProperties.getPgDB_user()
    val password = ReadProperties.getPgDB_password()

    //Setting connection properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)


    val sqlCommand = ParseUDFCommand.getSqlCommand()
    val inputList = ParseUDFCommand.getInputList()
    val outputTable = ParseUDFCommand.getOutputList()

    val numberOfTables = inputList.length

    val inputTables = new Array[String](numberOfTables)


    /* testing the connection to local test_db with jdbcDataFrame
    val jdbcDF1 = spark.read
      .jdbc(url, "student", connectionProperties)
      .createTempView("student")

    val jdbcDF2 = spark.read
      .jdbc(url, "grade", connectionProperties)
      .createTempView("grade")
    */
    // Saving data to a JDBC source

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

    executeCommand.show()
    executeCommand.explain()

    // Specifying create table column data types on write
    /*
    jdbcDF.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    */
  }

  /*
  def main(args: Array[String]): Unit = {

    val numberOfTalbes = 4;
    val sqlCmd = "select * from grade inner join student on grade.Stu_Id = student.Student_Id"

    val testSqlCmd =
      "select count(*) "+
        "from store_sales" +
        ", household_demographics" +
        ", time_dim" +
        ", store " +
        "where ss_sold_time_sk = time_dim.t_time_sk " +
        "and ss_hdemo_sk = household_demographics.hd_demo_sk " +
        "and ss_store_sk = s_store_sk and time_dim.t_hour = 8 " +
        "and time_dim.t_minute >= 30 and household_demographics.hd_dep_count = 5 " +
        "and store.s_store_name = 'ese' " +
        "order by count(*)"

    val inputList = new Array[String](numberOfTalbes)
    val outputList = "Destination of Table name"

    inputList(0) = "store_sales"
    inputList(1) = "household_demographics"
    inputList(2) = "time_dim"
    inputList(3) = "store"

    val LocalTableName = new Array[String](numberOfTalbes)

    for(i <- 0 until numberOfTalbes){
      LocalTableName(i) = inputList(i)
      println(LocalTableName(i))
    }

  }
  */
}
