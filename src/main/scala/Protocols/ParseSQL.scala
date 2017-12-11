package Protocols

import scala.collection.mutable.ArrayBuffer

object ParseSQL {
  private var sqlCommand = ""
  private var jobNo = 0;
  private var inputList = new ArrayBuffer[String]
  private var outputList = ""

  def parseSQLCommand(receivedRequest: String):Unit = {
    val tempCmd =  receivedRequest.split("%",0).toBuffer

    for(i <- 1 until tempCmd.length){
      i match {
        //case 0 => setCommandType(tempCmd(i))
        case 1 => setRequestNo(tempCmd(i))
        case 2 => setSqlCommand(tempCmd(i))
        case 3 => setInputList(tempCmd(i))
        case 4 => setOutputList(tempCmd(i))
      }
    }
  }

  def setRequestNo(str: String): Unit ={
    jobNo = str.toInt
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

  def getJobNo():Int ={
    return jobNo
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
