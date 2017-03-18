package com

import org.apache.spark.{SparkContext, SparkConf}

object OWL1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("OWL1")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/OWL1.in")

    val triples = lines.map(t => {
      val arr = t.split(" ")
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
      .filter(t => t._2.equals("rdf:type") && t._3.equals("owl:FunctionalProperty")).map(t => t._1)
    val funcPropSet = sc.broadcast(funcProp.collect.toSet)
    val funcTriple = triples.filter(t => funcPropSet.value.contains(t._2))
    val func1 = funcTriple.map(t => ((t._1, t._2), t._3))
    val result = func1.join(func1).map(t => (t._2._1, t._2._2)).filter(t => !t._1.equals(t._2))
    result.foreach(t => println(t))

    sc.stop()
  }
}
