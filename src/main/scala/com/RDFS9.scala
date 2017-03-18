package com

import org.apache.spark.{SparkContext, SparkConf}

object RDFS9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("RDFS9.in")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/RDFS9.in")

    val triples = lines.map(t => {
      val arr = t.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
u rdf:type v
v rdfs:subClassOf w
=>
u rdf:type w
     */

    val types = triples.filter(t => t._2.equals("rdf:type")).map(t => (t._1, t._3))
    val subClass = triples.filter(t => t._2.equals("rdfs:subClassOf")).map(t => (t._1, t._3))
    val reverseTypes = types.map(t => (t._2, t._1))
    val joined = subClass.join(reverseTypes)
    val res = joined.map(t => (t._2._2, t._2._1))

    res.foreach(t => println(t))
  }
}
