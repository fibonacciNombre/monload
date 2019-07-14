package com.test.load

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import com.test.load.model.PreCustomer
import com.typesafe.config.Config

import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

object MonLoad extends Job{
  
  override def appName: String = "monthly_load"
  
  override def run(spark:SparkSession,config: Config): Unit = {
    
    println("inicio --> " + appName)
    
    
    val rccDataRDD: RDD[String] = spark.sparkContext.textFile("./in/load_customer.csv")
    
    
    //rccDataRDD.foreach(f => println(f))
    
    val preCustRDD: RDD[(String, PreCustomer)] = rccDataRDD.map(f => {
                                          (f.split(";")(0), 
                                              PreCustomer.tupled(
                                                f.split(";")(0),
                                                f.split(";")(1)))
                                          }
                                        )
    
   // preCustRDD.foreach(f => println(f))
                                       
    
    println("file --> " + preCustRDD.count)
    
//    val readConfig2 = ReadConfig(Map("uri" -> "",
//                "database" -> "premia",
//                "collection" -> "coupons0117"
//                //"shardKey" -> "{_id:1,customerId:1}"
//                )) 
//
//    val ooo2 = spark.sparkContext.loadFromMongoDB(readConfig2)
//    
//    MongoSpark.save(ooo2)
    
    //println("db 099 --> " + ooo2.count)
    
    val readConfig = ReadConfig(Map("uri" -> "",
                "database" -> "movietickets",
                "collection" -> "movieTicket"
                //"shardKey" -> "{_id:1,customerId:1}"
                )) 

    val ooo = spark.sparkContext.loadFromMongoDB(readConfig)
    
    println("db 0 --> " + ooo.count)
//    
//    val rdd = MongoSpark.load(spark.sparkContext)
//    
//    println("db 1 --> " + rdd.count)
//    
//    val fromFile: RDD[(String, Document)] = rdd.map(f => (f.getString("customerId"),f))
//    
//    println("db 2 --> " + fromFile.count)
    //rdd.foreach(f => println(f))
//    
//    //fromFile.foreach(f => println(f))
//    
//    val uuu = preCustRDD.join(fromFile).map(f => {
//      
//      val pp = new Document("_id",f._2._2.get("_id"))
//      pp.put("customerId", f._1)
//      
//      val profi = f._2._1.profile.replace("[", "").replace("]", "").split(",").toList.asJava
//      pp.put("profile", profi)
//      pp.put("name", if(f._2._2.getString("name") != null) f._2._2.getString("name") else "nombre")
//      pp.put("lastName", if(f._2._2.getString("lastName") != null) f._2._2.getString("lastName") else "apellido")
//      pp.put("email", if(f._2._2.getString("email") != null) f._2._2.getString("email") else "email")
//      pp.put("active", if(f._2._2.getBoolean("active") != null) f._2._2.getBoolean("active") else false)
//      
//      pp
//    })
//    
////    val uuu = preCustRDD.leftOuterJoin(fromFile).map(f => {
////      
////     // val pp = new Document("_id",f._2._2.get.get("_id"))
////      val pp = new Document("customerId", f._1)
////      
////      val profi = f._2._1.profile.replace("[", "").replace("]", "").split(",").toList.asJava
////      pp.put("profile", profi)
////      pp.put("name",  "nombre")
////      pp.put("lastName",  "apellido")
////      pp.put("email",  "email")
////      pp.put("active", false)
////      
////      pp
////    })
//    
//    //uuu.foreach(f => println(f))
//    println("join -->" + uuu.count())
//    
//    val writeConfig = WriteConfig(Map("replaceDocument" -> "true", "shardKey" -> "{customerId:1}"), Some(WriteConfig(spark.sparkContext)))
//
//    MongoSpark.save(uuu,writeConfig)
//    
//    
//    
//    val doc = new Document("fruits", List("apples", "oranges", "pears").asJava)
//    
//    doc.put("customerId", "0060089638")
//    
//    println(doc)
//    println(doc.get("fruits"))
//    println(rdd.first.toJson)
//    
//    
  }
}