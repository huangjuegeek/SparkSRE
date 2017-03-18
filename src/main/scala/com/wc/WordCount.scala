package com.wc

import org.apache.spark.{SparkContext, SparkConf}

object WordCount {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf()
    conf.setAppName("MyWordCount")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("wordcount.txt")
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.foreach(w => println(w._1 + " " + w._2))
    sc.stop()
  }
}
