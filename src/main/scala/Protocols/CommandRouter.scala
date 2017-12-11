package Protocols

import Protocols.ParseSQL._
import Execution.SparkSQL
import org.apache.spark.sql.SparkSession
object CommandRouter {

  def router(receivedRequest: String, spark: SparkSession):Unit = {
    val tempCmd =  receivedRequest.split("%",0).toBuffer

    if(tempCmd(0) matches("SQL")){
      ParseSQL.parseSQLCommand(receivedRequest)
      SparkSQL.executeSqlCommand(spark)
    }else
    if(tempCmd(0) matches("LR")){
      println("Later...")
    }
    else println("ILLEGAL REQUEST: "+tempCmd(1)+". Please check the header info.")

  }
}
