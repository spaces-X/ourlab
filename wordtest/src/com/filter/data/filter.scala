package com.filter.data


import java.io.{StringReader, StringWriter}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import scala.collection.JavaConversions._
import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
object filter {
  def main(args: Array[String]): Unit = {
   
    val conf = new SparkConf().setMaster("spark://bigdata02:7077").setAppName("all daybytype");
    val sc = new SparkContext(conf);
    if(args.length!=6){
      println(args.length+"is not suit for this program")
      println(" please input c and the price of diffent type '11 12 13 14 15' ")
      exit(0)
    }
    val lines = sc.textFile("hdfs://bigdata01:9000/home/guizhou/csv/exlist*201701.csv")
    val head = lines.take(2)
    var filterline = lines.filter{
      x=>
        val items = x.split(",")
          (items.length==83 || items.length==100) && items(0)!="TOLLRATEVER" && items(0)!="-----------"
    }
    filterline = filterline.filter{
      x=>
        val items = x.split(",")
        items(3).length>0 && items(14).length>0 && items(16).length>0 && items(23).length>0 && items(20).toInt>10 && items(20).toInt<16 && items(23).toDouble>0
    }
    var mapper = filterline.map{   // 保留有用的字段
      x=>
        val items = x.split(",")
        var 
    }
  }
}