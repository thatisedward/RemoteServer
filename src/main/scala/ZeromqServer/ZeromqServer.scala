package ZeromqServer

/**
  * Created by Edward Zhang.
  */
import Protocols._
import org.apache.spark.sql.SparkSession
import LoadPgDBConf.ReadProperties
import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Socket}

object ZeromqServer {
  def main(args : Array[String]) {

    val spark = SparkSession.builder()
        .master("local")
        .getOrCreate()

    val context = ZMQ.context(1)
    val socket = context.socket(ZMQ.REP)
    print ("The ZMQ server starting ")
    val address = "tcp://*:"+ReadProperties.getZmq_port()
    socket.bind(address)
    println ("on port: "+ ReadProperties.getZmq_port())
    //socket.bind ("tcp://*:5555")

    while (true) {
      //  Wait for next request from client
      val request = socket.recv (0)
      //  In order to display the 0-terminated string as a String,
      //  we omit the last byte from request
      //  Creates a String from request, minus the last byte
      val receivedRequest = new String(request,0,request.length-1)

      println ("Received request: ["
        + receivedRequest
        + "]")

      CommandRouter.router(receivedRequest, spark)

      val reply = ("Job "+ParseSQL.getJobNo()+" is finished.").getBytes
      reply(reply.length-1)=0
      socket.send(reply, 0)

    }
  }
}
