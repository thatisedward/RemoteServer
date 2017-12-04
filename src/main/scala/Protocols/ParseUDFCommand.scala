package Protocols

import scala.collection.mutable.ArrayBuffer

object ParseUDFCommand {
  private var sqlCommand = ""
  private var inputList = new ArrayBuffer[String]
  private var outputList = ""

  def parseUDFCommand(receivedRequest: String):Unit = {
    val tempCmd =  receivedRequest.split("%",0).toBuffer

    for(i <- 0 until tempCmd.length){
      i match {
        case 0 => setSqlCommand(tempCmd(i))
        case 1 => setInputList(tempCmd(i))
        case 2 => setOutputList(tempCmd(i))
      }
    }
  }

  def setSqlCommand(str: String):Unit = {
    sqlCommand = str
  }

  def setInputList(str: String):Unit = {
    val tsl = str.split(",",0).toBuffer
    for(i <- 0 until tsl.length)
      inputList += tsl(i)
  }

  def setOutputList(str: String):Unit ={
    outputList = str
  }

  def getSqlCommand():String = {
    return sqlCommand
  }

  def getInputList():ArrayBuffer[String] ={
    return inputList
  }

  def getOutputList():String = {
    return outputList
  }

}
