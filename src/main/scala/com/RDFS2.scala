package com

import org.apache.spark.{SparkContext, SparkConf}

object RDFS2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("RDFS2.in")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/RDFS2.in")

    val triples = lines.map(t => {
      val arr = t.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
p rdfs:domain x
s p o
=>
s rdf:type x
     */

    val domain = triples.filter(t => t._2.equals("rdfs:domain")).map(t => (t._1, t._3))
    val pso = triples.map(t => (t._2, (t._1, t._3)))
    val joined = pso.join(domain)
    val res = joined.map(t => (t._2._1._1, t._2._2))

    res.foreach(t => println(t))
  }
}
