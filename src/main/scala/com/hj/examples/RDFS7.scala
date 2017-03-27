package com.hj.examples

import com.hj.constant.Const
import org.apache.spark.{SparkConf, SparkContext}

object RDFS7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDFS7").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/RDFS7.in")

    val triples = lines.map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
s p o
p rdfs:subPropertyOf q
=>
s q o
     */

    val subProp = triples.filter(x => x._2.equals(Const.RDFS_SUBPROPERTY_OF)).map(x => (x._1, x._3))
    val pso = triples.map(x => (x._2, (x._1, x._3)))
    val joined = pso.join(subProp)
    val res = joined.map(x => (x._2._1._1, x._2._2, x._2._1._2))

    res.foreach(t => println(t))
  }
}
