
name := "RemoteServer"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0" //% "provided"

libraryDependencies += "org.postgresql" % "postgresql" % "42.1.4"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "org.zeromq" % "jeromq" % "0.3.6"
