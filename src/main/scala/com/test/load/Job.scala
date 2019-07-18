package com.test.load

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

trait Job {
  def main(args: Array[String]): Unit = {
    
    val config = ConfigFactory.load()
    
    val spark = SparkSession.builder()
        .appName(appName)
        .master("local[1]")//comentar al desplegar en spark
        //.config("spark.cassandra.connection.host", config.getString("bd.cassandra.url"))
        //.config("spark.mongodb.input.uri", "")
        //.config("","")
        //.config("spark.mongodb.output.uri", "")

        .getOrCreate()
        
    parseAndRun(spark, args, config)
    
    def parseAndRun(spark: SparkSession, args: Array[String], config: Config): Unit = {
//      new UsageOptionParser().parse(args, UsageConfig()) match {
//        case Some(config) => run(spark, config, new S3Storage(spark))
//        case None => throw new IllegalArgumentException("arguments provided to job are not valid")
//      }
      run(spark,config)
    }
    
  }
  
  def run(spark: SparkSession, config: Config)
  
  def appName: String
}