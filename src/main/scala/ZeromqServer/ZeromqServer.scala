package ZeromqServer

/**
  * Created by Edward Zhang.
  */
import ExecuteOnSpark.ExecuteSqlCommand
import Protocols.ParseUDFCommand
import org.apache.spark.sql.SparkSession
import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Socket}

object ZeromqServer {
  def main(args : Array[String]) {

    val spark = SparkSession.builder()
        .master("local")
        .getOrCreate()

    val context = ZMQ.context(1)
    val socket = context.socket(ZMQ.REP)
    println ("starting")
    socket.bind ("tcp://*:5555")

    while (true) {
      //  Wait for next request from client
      //  We will wait for a 0-terminated string (C string) from the client,
      val request = socket.recv (0)
      //  In order to display the 0-terminated string as a String,
      //  we omit the last byte from request
      //  Creates a String from request, minus the last byte
      val receivedRequest = new String(request,0,request.length-1)

      println ("Received request: ["
        + receivedRequest
        + "]")

      ParseUDFCommand.parseUDFCommand(receivedRequest)
      ExecuteSqlCommand.executeSqlCommand(spark)

      //  Do some 'work'
      try {
        Thread.sleep (1000)
      } catch  {
        case e: InterruptedException => e.printStackTrace()
      }

      val reply = "The request has been received ... ".getBytes
      reply(reply.length-1)=0 //Sets the last byte of the reply to 0
      socket.send(reply, 0)

    }
  }
}
