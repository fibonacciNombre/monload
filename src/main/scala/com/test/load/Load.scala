package com.test.load

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document

import com.mongodb.spark._
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
import com.test.load.model.FileCustomer
import java.util.Calendar

object Load extends Job{
  
  override def appName: String = "monthly_load"
  
  override def run(spark:SparkSession,config: Config): Unit = {
    
    
    
    println("inicio --> " + appName)
    val now = Calendar.getInstance().getTime()
    println("inicio -- " + now)
     
    //desde el Blob
    spark.sparkContext.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.staeu2c002prmadev02.blob.core.windows.net", "")
   
     val path = "wasbs://premia-carga@staeu2c002prmadev02.blob.core.windows.net"
     
 
     
    //  val azureRDD = spark.sparkContext.textFile(path + "/backup/Customer201907.csv")
     
     
    // azureRDD.foreach(f => println(f))
     
     
    import spark.sqlContext.implicits._
    
  //  val rccDataRDD: RDD[String] = spark.sparkContext.textFile("./in/customer0117.csv")
    val rccDataRDD: RDD[String] = spark.sparkContext.textFile(path + "/monthly/load_customer.csv")
    println("nuevos --> " + rccDataRDD.count())
    //rccDataRDD.foreach(f => println(f))
    

 
    
    val preCustRDD: RDD[(String, PreCustomer)] = rccDataRDD.map(f => {
                                          (f.split(";")(0), 
                                              PreCustomer.tupled(
                                                f.split(";")(0),
                                                f.split(";")(1)))
                                          }
                                        ).reduceByKey((a,b) => a)
    
    //preCustRDD.foreach(f => println(f))
                                       
   
    
    println("file --> " + preCustRDD.count)
    
    val readConfig2 = ReadConfig(Map(
                "uri" -> "",               
                "database" -> "premia",
                "collection" -> "customer0"
                )) 

    //val rdd = spark.sparkContext.loadFromMongoDB(readConfig2)
    
    //val rdd: RDD[String] = spark.sparkContext.textFile("./in/big_customer.csv")
    val rdd = spark.sparkContext.textFile(path + "/backup/Customer201907.csv")
    
    println("db --> " + rdd.count)
    

////    
//    println("db 1 --> " + rdd.count)
//    
    val fromFile: RDD[(String, FileCustomer)] = rdd.map(f => (f.split(";")(0),FileCustomer.tupled(
          f.split(";")(0),
          f.split(";")(1),
          f.split(";")(2),
          f.split(";")(3),
          true)))
//    
//    println("db 2 --> " + fromFile.count)
    //rdd.foreach(f => println(f))
//    
   // fromFile.foreach(f => println("parrdd bd -->" + f))
////  
          
    val uuu = preCustRDD.join(fromFile).map(f => {
      
      val pp = new Document()
      pp.put("customerId", f._1)
      
      val profi = f._2._1.profile.replace("[", "").replace("]", "").split(",").toList.asJava
      pp.put("profiles", profi)
      pp.put("name", if(f._2._2.nombre != null) f._2._2.nombre else "nombre")
      pp.put("lastName", if(f._2._2.apellido != null) f._2._2.apellido else "apellido")
      pp.put("email", if(f._2._2.correo != null) f._2._2.correo else "email")
      pp.put("active", true)//if(f._2._2.getBoolean("active") != null) f._2._2.getBoolean("active") else false)
      
      (pp)
    })
    

        
    
    
//    
    println("join")
   // uuu.foreach(f =>  println("join -->" + f))
//    
    
    val lefttodo = fromFile.leftOuterJoin( preCustRDD).filter(f => !f._2._2.nonEmpty).map(f => {(f._2._1)}).map(f =>{
      val hhh = new Document("customerId", f.id)
      val profi = "DEFAULT".split(",").toList.asJava
      hhh.put("profiles", profi)
      hhh.put("name", f.nombre)
      hhh.put("lastName", f.apellido)
      hhh.put("email", f.correo)
      hhh.put("active", false)//if(f._2._2.getBoolean("active") != null) f._2._2.getBoolean("active") else false)
      
      hhh
    })
    
    //lefttodo.foreach(f =>  println("leftJoin -->" + f))
    
    val kkk = uuu.union(lefttodo)
    
    
   // println("leftJoin --> " + kkk.count())
 
    println("todo -->" + kkk.count())
    kkk.foreach(f => println("final -->" + f))
   // kkk.saveAsTextFile("./in/todo.txt")
    
//    MongoConnector(readConfig2).withDatabaseDo(readConfig2, db => db.getCollection("customer0117").deleteMany(new Document))
//    
//    println("todo -->" + kkk.count())
//        val writeConfig = WriteConfig(Map(
//                "uri" -> "",
//                "database" -> "premia",
//                "collection" -> "customer0"
//                )) 
//
//    MongoSpark.save(kkk,writeConfig)
    
     val fin = Calendar.getInstance().getTime()
    println("inicio -- " + fin)
    
    

  }
  
}