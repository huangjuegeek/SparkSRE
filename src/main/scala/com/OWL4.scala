package com

import org.apache.spark.{SparkContext, SparkConf}

object OWL4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("OWL4.in")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/OWL4.in")

    val triples = lines.map(t => {
      val arr = t.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
p rdf:type owl:TransitiveProperty
u p w
w p v
=>
u p v
     */
    val transProp = triples.filter(t => t._3.equals("owl:TransitiveProperty")).map(t => t._1).collect()

    val tp = sc.broadcast(transProp.toSet)
    val transSPO = triples.filter(t => tp.value.contains(t._2))
    tp.value.foreach(t => {
      val curSPO = transSPO.filter(spo => spo._2.equals(t))
      var result = curSPO
      var inc = result
      var left = inc.map(x => ((x._2, x._3), x._1))
      var incCount = 0L
      do {
        var right = inc.map(t => ((t._2, t._1), t._3))
        val phase1 = left.join(right).map(t => (t._2._1, t._1._1, t._2._2))
        inc = phase1.subtract(result)
        incCount = inc.count()
        if(incCount != 0) {
          left = inc.map(t => ((t._2, t._3), t._1))
          right = result.map(t => ((t._2, t._1), t._3))
          val phase2 = left.join(right).map(t => (t._2._1, t._1._1, t._2._2))
          result = phase2.union(inc).union(result)
        }
      } while(incCount != 0)
      result.foreach(x => println(x))
    })

    sc.stop()
  }
}
