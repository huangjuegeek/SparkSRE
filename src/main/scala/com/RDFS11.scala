package com

import org.apache.spark.{SparkContext, SparkConf}

object RDFS11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("RDFS11")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/RDFS11.in")

    val triples = lines.map(t => {
      val arr = t.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
  u rdfs:subClassOf v
  v rdfs:subClassOf w
  =>
  u rdfs:subClassOf w
     */

    var subClass = triples.filter(t => t._2.equals("rdfs:subClassOf")).map(t => (t._1, t._3))
    val reverseSubClass = subClass.map(t => (t._2, t._1))

    var cur = 0L
    var pre = subClass.count()
    var flag = true
    while (flag) {
      val joined = reverseSubClass.join(subClass)
      val res = joined.map(t => t._2)
      subClass = subClass.union(res).distinct
      cur = subClass.count()
      if (pre == cur) flag = false
      pre = cur
    }

    subClass.foreach(t => println(t))
  }
}
