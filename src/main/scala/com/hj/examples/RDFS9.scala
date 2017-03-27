package com.hj.examples

import com.hj.constant.Const
import org.apache.spark.{SparkConf, SparkContext}

object RDFS9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDFS9.in").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/RDFS9.in")

    val triples = lines.map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
u rdf:type v
v rdfs:subClassOf w
=>
u rdf:type w
     */

    val types = triples.filter(x => x._2.equals(Const.RDF_TYPE)).map(x => (x._1, x._3))
    val subClass = triples.filter(x => x._2.equals(Const.RDFS_SUBCLASS_OF)).map(x => (x._1, x._3))
    val reverseTypes = types.map(x => (x._2, x._1))
    val joined = subClass.join(reverseTypes)
    val res = joined.map(x => (x._2._2, x._2._1))

    res.foreach(x => println(x))
  }
}
