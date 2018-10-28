package com.ele.day

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object wordtest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("spark://bigdata02:7077").setAppName("test");
    val sc = new SparkContext(conf);
    val text_file = sc.textFile("hdfs://bigdata01:9000/home/weixiang/input/a.txt");
    text_file.take(5);
    val words = text_file.flatMap(line => line.split(" "));
    val counts = words.map(word=>(word,1)).reduceByKey{case(x,y)=>x+y}
    counts.saveAsTextFile("hdfs://bigdata01:9000/home/weixiang/output/02/spark")
//  counts.saveAsTextFile("")
    
    println("wasdasd");
  }
}