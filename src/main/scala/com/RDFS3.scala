package com

import org.apache.spark.{SparkContext, SparkConf}

object RDFS3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("RDFS3.in")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/RDFS3.in")

    val triples = lines.map(t => {
      val arr = t.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
p rdfs:range x
s p o
=>
o rdf:type x
     */

    val range = triples.filter(t => t._2.equals("rdfs:range")).map(t => (t._1, t._3))
    val pso = triples.map(t => (t._2, (t._1, t._3)))
    val joined = pso.join(range)
    val res = joined.map(t => (t._2._1._2, t._2._2))

    res.foreach(t => println(t))
  }
}
