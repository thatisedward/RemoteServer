package LoadPgDBConf

import com.typesafe.config.{Config, ConfigFactory}

object ReadProperties {

  val config: Config = ConfigFactory.load("testPgDB.conf")

  def getPgDB_url(): String = {
    config.getString("PgDB_url")
  }

  def getPgDB_user(): String = {
    config.getString("PgDB_user")
  }

  def getPgDB_password(): String = {
    config.getString("PgDB_password")
  }

  /*
  def getSparkMaster_address(): String={
    config.getString("SparkMaster_address")
  }

  def getSparkMaster_port(): Int = {
    config.getInt("SparkMaster_port")
  }
  */
}
