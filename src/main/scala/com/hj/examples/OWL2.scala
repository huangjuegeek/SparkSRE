package com.hj.examples

import org.apache.spark.{SparkConf, SparkContext}

object OWL2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("OWL2")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/OWL2.in")

    val triples = lines.map(t => {
      val arr = t.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
p rdf:type owl:InverseFunctionalProperty
v p u
w p u
=>
v owl:sameAs w
     */

    val invFuncProp = triples
      .filter(t => t._2.equals("rdf:type") && t._3.equals("owl:InverseFunctionalProperty")).map(t => t._1)
    val invFuncPropSet = sc.broadcast(invFuncProp.collect.toSet)
    val invFuncTriple = triples.filter(t => invFuncPropSet.value.contains(t._2))
    val func1 = invFuncTriple.map(t => ((t._2, t._3), t._1))
    val result = func1.join(func1).map(t => (t._2._1, t._2._2)).filter(t => !t._1.equals(t._2))
    result.foreach(t => println(t))

    sc.stop()
  }
}
