package org.ditosoft.repository

import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._

object CassandraRepository {

  def saveOnCassandra(df: DataFrame, tableName:String, keySpace: String)(implicit spark:SparkSession): Unit ={
    df.write
      .mode(SaveMode.Append)
      .cassandraFormat(tableName, keySpace)
      .save()
  }

  def readFromCassandra(tableName:String, keySpace: String)(implicit spark:SparkSession): DataFrame ={
    spark
      .read
      .cassandraFormat(tableName, keySpace)
      .options(ReadConf.SplitSizeInMBParam.option(32))
      .load()
  }


}
