package com.hj.examples

import org.apache.spark.{SparkConf, SparkContext}

object OWL4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OWL4.in").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/OWL4.in")

    val triples = lines.map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
p rdf:type owl:TransitiveProperty
u p w
w p v
=>
u p v
     */
    val transProp = triples.filter(x => x._3.equals("owl:TransitiveProperty")).map(x => x._1).collect()
    val tp = sc.broadcast(transProp.toSet)

    val transSPO = triples.filter(x => tp.value.contains(x._2))
    tp.value.foreach(x => {
      val curSPO = transSPO.filter(spo => spo._2.equals(x))
      var result = curSPO
      var inc = result
      var left = inc.map(x => ((x._2, x._3), x._1))
      var incCount = 0L
      do {
        var right = inc.map(x => ((x._2, x._1), x._3))
        val phase1 = left.join(right).map(x => (x._2._1, x._1._1, x._2._2))
        inc = phase1.subtract(result)
        incCount = inc.count()
        if(incCount != 0) {
          left = inc.map(x => ((x._2, x._3), x._1))
          right = result.map(x => ((x._2, x._1), x._3))
          val phase2 = left.join(right).map(x => (x._2._1, x._1._1, x._2._2))
          result = phase2.union(inc).union(result)
        }
      } while(incCount != 0)
      result.foreach(x => println(x))
    })

    sc.stop()
  }
}
