package Protocols

import Protocols.ParseSQL._
import Execution.SparkSQL
import org.apache.spark.sql.SparkSession
import org.zeromq.ZMQ.Socket
object CommandRouter {

  def router(receivedRequest: String, spark: SparkSession, socket: Socket):Unit = {
    val tempCmd =  receivedRequest.split("%",0).toBuffer

    if(tempCmd(0) matches("SQL")){

      ParseSQL.parseSQLCommand(receivedRequest)
      SparkSQL.executeSqlCommand(spark,socket)

    }else
    if(tempCmd(0) matches("LR")){
      println("Later...")
    }
    else {
      val print1 = "\n-----ILLEGAL REQUEST-----\n"+receivedRequest+".\n-----Please Check-----!\n"
      val print2 = "\nThe format should be something as \"SQL%1234%select * from ...%table_name%output_table\"...\n"

      println(print1)
      println(print2)

      val reply = (print1+print2).getBytes()
      reply(reply.length-1) = 0
      socket.send(reply)
    }

  }
}
