package com

import org.apache.log4j.{LogManager, Level}
import org.apache.spark.{SparkContext, SparkConf}

object RDFS5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("RDFS5")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/RDFS5.in")

    val triples = lines.map(t => {
      val arr = t.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
p rdfs:subPropertyOf q
q rdfs:subPropertyOf r
=>
p rdfs:subPropertyOf r
     */

    var subProp = triples.filter(t => t._2.equals("rdfs:subPropertyOf")).map(t => (t._1, t._3))
    val reverseSubProp = subProp.map(t => (t._2, t._1))

    var cur = 0L
    var pre = subProp.count()
    var flag = true
    while (flag) {
      val joined = reverseSubProp.join(subProp)
      val res = joined.map(t => t._2)
      subProp = subProp.union(res).distinct
      cur = subProp.count()
      if(pre == cur) flag = false
      pre = cur
    }

    subProp.foreach(t => println(t))
  }
}
