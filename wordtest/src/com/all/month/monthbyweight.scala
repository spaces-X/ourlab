package com.all.month

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

/*
 * 按照车重和日期统计收费金额 
 * 日期粒度：每月
 * */

object monthbyweight {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("spark://bigdata02:7077").setAppName("elemonthbyweight");
    val sc = new SparkContext(conf);
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
        items(4).length>0 && items(14).length>0 && items(16).length>0 && items(23).length>0 && items(20).length>1 && items(23).toDouble>0
    }
    val mapped = filterline.map{
      x=>
        val items = x.split(",")
        val time = items(16).substring(0, 7)    // 精确到“月份”
        val vtype = items(20)
        val cash = items(23).toDouble
        ((time,vtype),cash)
    }
    val txt = mapped.reduceByKey((x,y)=>x+y)
    txt.repartition(1).sortByKey(true)
    .map(x=>List(x._1._1,x._1._2,x._2.toString()).toArray).mapPartitions{
      data=>
        val stringWriter = new StringWriter()
        val csvWriter = new CSVWriter(stringWriter)
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
    }.saveAsTextFile("hdfs://bigdata01:9000/home/guizhou/all/All_monthvtype_byweight")
    //txt.repartition(1).sortByKey(true).saveAsTextFile("hdfs://bigdata01:9000/home/guizhou/Elec_monthvtype_byweight")
    
  }
}