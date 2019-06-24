package org.ditosoft

import org.apache.spark.sql.SparkSession
import org.ditosoft.entity.User
import org.ditosoft.repository.CassandraRepository
import org.apache.spark.sql.functions.col

object Main  extends App {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark_Cassandra")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    val tableName = "user"
    val keySpaceName = "public"

    val users = Seq(
        User("usuario2", "Manjarres", "Nala", "Holanda", 12),
        User("usuario3", "Perez", "Karla", "Chile", 13),
        User("usuario4", "Gonzales", "Princesa", "España", 34),
        User("usuario5", "Gutierrez", "Nata", "Alemania", 25)
    )
    val usersRDD= spark.sparkContext.parallelize(users)
    val usersToSaveDF = spark.createDataFrame(usersRDD)

    CassandraRepository.saveOnCassandra(usersToSaveDF, tableName, keySpaceName)

    val usersDf = CassandraRepository.readFromCassandra(tableName, keySpaceName)

    usersDf.filter(col("edad")<20).show()
    usersDf.show()

}
