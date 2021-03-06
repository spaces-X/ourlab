package com.sensitive.c

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
import shapeless.ops.nat.ToInt


object potential_loss {
  def main(args: Array[String]): Unit = {
    if (args.length!=6)
    {
      println("your args length "+args.length+" is not suit for this program")
      println("please add the argument of sensitive factor 'c' and 5 prices of different type")
      exit(1)
    }
    val conf = new SparkConf().setMaster("spark://bigdata02:7077").setAppName("all daybytype");
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
//    var chargebytype = Array(0.52,1.22,2.1,2.62,3.32)     // 最低档 贵州
    var buffer = args.toBuffer
    buffer.remove(0)
    var chargebytype = buffer.toArray.map(_.toDouble)
    val dismapped = filterline.map{           // map 出按type收费亏了的车的（cash，1）cash 和车次
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
        var cash_bytype = chargebytype(vtype.toInt-11)*(dis/1000)
        var cash = items(23).toDouble
        var factor_c = args(0).toDouble
        var flag_loss =cash_bytype-cash*(1+factor_c) > 0
        var flag=0
        if (flag_loss)
        {
          flag=1
        }
        ((time,vtype),(cash,flag))
    }
    var reducer = dismapped.filter(_._2._2==1).reduceByKey{                    // reducebykey 
      (x,y)=>
        (x._1+y._1,x._2+y._2)
    }
    reducer.repartition(1).sortByKey(true)
    .map(x=>List(x._1._1,x._1._2,x._2._1.toString(),x._2._2.toString()).toArray).mapPartitions{
      data=>
        val stringWriter = new StringWriter()
        val csvWriter = new CSVWriter(stringWriter)
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
    }.saveAsTextFile("hdfs://bigdata01:9000/home/guizhou/potential/"+"c"+args(0)+"/"+args(1)+"/All_dayvtype_byvtype")
    println("c price:"+args)
  
  
  }
  
  
}