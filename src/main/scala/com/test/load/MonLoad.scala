package com.test.load

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

object MonLoad extends Job{
  
  override def appName: String = "monthly_load"
  
  override def run(spark:SparkSession,config: Config): Unit = {
    
    println("inicio --> " + appName)
    
  }
}