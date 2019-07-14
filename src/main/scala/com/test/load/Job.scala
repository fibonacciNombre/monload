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
        .config("spark.mongodb.input.uri", "mongodb://acdbeu2c002prmadev01:XaXUl8IhJXFToMhYB92SebetDErUlTB1hGODqdAPLwhryMf2dRfA8SWaxe4usgttJSW9GCKwqqfdpUokfjantQ==@acdbeu2c002prmadev01.documents.azure.com:10255/premia.client?ssl=true&replicaSet=globaldb")
        .config("spark.mongodb.output.uri", "mongodb://acdbeu2c002prmadev01:XaXUl8IhJXFToMhYB92SebetDErUlTB1hGODqdAPLwhryMf2dRfA8SWaxe4usgttJSW9GCKwqqfdpUokfjantQ==@acdbeu2c002prmadev01.documents.azure.com:10255/premia.coupons0117?ssl=true&replicaSet=globaldb")
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