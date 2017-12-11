package Protocols

import Protocols.ParseSQLCommand._

object CommandRouter {

  def router(receivedRequest: String):Unit = {
    val tempCmd =  receivedRequest.split("%",0).toBuffer

    if(tempCmd(0) matches("SQL")){
      ParseSQLCommand.parseSQLCommand(receivedRequest)
    }else
    if(tempCmd(0) matches("LR")){
      println("Later...")
    }
    else println("ILLEGAL REQUEST: "+tempCmd(1)+". Please check the header info.")

  }
}
