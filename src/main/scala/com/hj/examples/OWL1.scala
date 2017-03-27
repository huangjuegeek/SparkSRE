package com.hj.examples

import org.apache.spark.{SparkConf, SparkContext}

object OWL1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OWL1").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/OWL1.in")

    val triples = lines.map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
p rdf:type owl:FunctionalProperty
u p v
u p w
=>
v owl:sameAs w
 */
    val funcProp = triples
      .filter(x => x._2.equals("rdf:type") && x._3.equals("owl:FunctionalProperty")).map(x => x._1)
    val funcPropSet = sc.broadcast(funcProp.collect.toSet)
    val funcTriple = triples.filter(x => funcPropSet.value.contains(x._2))
    val func1 = funcTriple.map(x => ((x._1, x._2), x._3))
    val result = func1.join(func1).map(x => (x._2._1, x._2._2)).filter(x => !x._1.equals(x._2))
    result.foreach(x => println(x))

    sc.stop()
  }
}
