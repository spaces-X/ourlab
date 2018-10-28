package com.ele.day

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/*
 * 按照车重和日期统计收费金额 
 * 日期粒度：每日
 * */
object daybyweight {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("spark://bigdata02:7077").setAppName("eledaybyweight");
    val sc = new SparkContext(conf);
    val lines = sc.textFile("hdfs://bigdata01:9000/home/guizhou/csv/exlistelec201701.csv")
    val head = lines.take(2)
    var filterline = lines.filter{
      x=>
        val items = x.split(",")
        !head.contains(x) && items.length==100
    }
    filterline = filterline.filter{
      x=>
        val items = x.split(",")
        items(4).length>0 && items(14).length>0 && items(16).length>0 && items(23).length>0 && items(20).length>1 && items(23).toDouble>0
    }
    val mapped = filterline.map{
      x=>
        val items = x.split(",")
        val time = items(16).substring(0, 10)
        val vtype = items(20)
        val cash = items(23).toDouble
        ((time,vtype),cash)
    }
    val txt = mapped.reduceByKey((x,y)=>x+y)
    txt.repartition(1).sortByKey(true, 1).saveAsTextFile("hdfs://bigdata01:9000/home/guizhou/Elec_dayvtype_byweight")
    
  }
}