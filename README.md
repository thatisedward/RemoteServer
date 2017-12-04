# RemoteServer

The idea of this project aims to execute a sql command on a spark cluster to extend the utilization and maximize the performance of a postgres database when dealing with massive data operations.

Main functions:

-- The remote server uses ZerMQ to have a message interaction with clients (postgres database).

-- The remote server uses JdbcDF to have a data interaction (both read and wirte) with postgres database.

Processes:

-- The zmq socket waits for the request after the zmqContext has been established.

-- The received request will be parsed into three fields: sqlCommand, inputlist, outputlist.

-- The spark session reads the PgDB properties from the .conf in the resources folder and uses read.jdbc to start a connection to the PgDB.

-- Tables from the PgDB can be loaded as a temporary view using jdbc.createTempView

-- The sql command could be executed by spark.sql.

-- The result could be saving back to the PgDB by write.jdbc.

The sbt-assembly is used for packing the dependencies.
 
