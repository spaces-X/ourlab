package com.cash.day
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
 * 按照车型和日期来统计收费情况
 * 日期粒度：每日
 * */

object daybytype {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("spark://bigdata02:7077").setAppName("eledaybytype");
    val sc = new SparkContext(conf);
    val lines = sc.textFile("hdfs://bigdata01:9000/home/guizhou/csv/exlistcash201701.csv")
    val head = lines.take(2)
    var filterline = lines.filter{
      x=>
        val items = x.split(",")
        !head.contains(x) && items.length==83
    }
    filterline = filterline.filter{
      x=>
        val items = x.split(",")
        items(3).length>0 && items(14).length>0 && items(16).length>0 && items(23).length>0 && items(20).toInt>10 && items(20).toInt<16 && items(23).toDouble>0
    }
    var dislines = sc.textFile("hdfs://bigdata01:9000/home/guizhou/csv/dis_station.csv")   // 站间距离表格
    var temp = dislines.first()
    dislines = dislines.filter{
      x=>
        x.split(',').length==5 && x!=temp
    }
    val distance = dislines.map{
      x=>
        val items = x.split(',')
        val origin = items(3)
        val dest = items(4)
        val dis = items(2).toDouble
        ((origin,dest),dis)
    }
    var dismap = distance.collectAsMap() // 得到 不同站点编号到距离的映射(string,string) -> double 
//    val distance:Map[(String,String),Double] = Map()
//    val num = dislines.count().toInt    // 精度损失  文件不大的话问题不大  
//    val dislineslist = dislines.take(num)
//    var i =dislineslist(0)
//    
    var chargebytype = Array(0.5,1,1.5,1.8,2)
    val dismapped = filterline.map{
      x=>
        val items = x.split(",")
        val enstation = items(4)
        val exstation = items(14)
        var dis = -1.000
        if (dismap.contains((enstation,exstation))) {
          dis = dismap((enstation,exstation))
        }
        val time = items(16).substring(0, 10)
        val vtype = items(20)
        var cash = chargebytype(vtype.toInt-11)*(dis/1000)
        ((time,vtype),cash)
        
    }
    val outfile = dismapped.reduceByKey((x,y)=>(x+y))
//    outfile.repartition(1).sortByKey(true).saveAsTextFile("hdfs://bigdata01:9000/home/guizhou/Elec_dayvtype_byvtype")
    outfile.repartition(1).sortByKey(true)
    .map(x=>List(x._1._1,x._1._2,x._2.toString()).toArray).mapPartitions{
      data=>
        val stringWriter = new StringWriter()
        val csvWriter = new CSVWriter(stringWriter)
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
    }.saveAsTextFile("hdfs://bigdata01:9000/home/guizhou/cash/Cash_dayvtype_byvtype")
  }
}