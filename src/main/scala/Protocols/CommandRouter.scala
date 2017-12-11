package Protocols

import Protocols.ParseSQL._

object CommandRouter {

  def router(receivedRequest: String):Unit = {
    val tempCmd =  receivedRequest.split("%",0).toBuffer

    if(tempCmd(0) matches("SQL")){
      ParseSQL.parseSQLCommand(receivedRequest)
    }else
    if(tempCmd(0) matches("LR")){
      println("Later...")
    }
    else println("ILLEGAL REQUEST: "+tempCmd(1)+". Please check the header info.")

  }
}
