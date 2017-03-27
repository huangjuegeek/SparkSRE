package com.hj.examples

import org.apache.spark.{SparkConf, SparkContext}

object OWL7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("OWL7")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/OWL7.in")

    val triples = lines.map(t => {
      val arr = t.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
v owl:sameAs w
w owl:sameAs u
=>
v owl:sameAs u
     */
    val sameAsTriples = triples.filter(t => t._2.equals("owl:sameAs"))
    var same = sameAsTriples.map(t => (t._1, t._3))
    var countBefore = 0L
    var countAfter = 0L
//    var id: RDD[(String, String)] = sc.emptyRDD[(String, String)]
    do {
      countBefore = same.count
      val sameIter = same
      same = sameIter.map(t => (t._2, t._1)).union(sameIter).groupByKey()
        .flatMap(t => {
          val minSecond = t._2.min
          if(t._1.compareTo(minSecond) < 0) {
            for(x <- t._2) yield (x, t._1)
          } else {
            for(x <- t._2) yield (x, minSecond)
          }
        })
      same = same.filter(x => !x._1.equals(x._2))
      countAfter = same.count
    } while(countBefore < countAfter)

    same = same.distinct()
    same = same.sortBy(x => x._2)

    var resourceToId = same
    var idToResource = resourceToId.map(t => (t._2, t._1)).groupByKey()
    val idToId = idToResource.map(t => (t._1, t._1))
    resourceToId = resourceToId.union(idToId)
    idToResource = resourceToId.map(t => (t._2, t._1)).groupByKey()

    resourceToId.foreach(t => println(t))
    println("-----")
    idToResource.foreach(t => println(t))
    println("-----")

    val resource_to_id = resourceToId.collectAsMap()
    val id_to_resource = idToResource.collectAsMap()

    //OWL11
    val spo = triples.subtract(sameAsTriples)

    val inc = spo.foreach(t => {
      val subjId = resource_to_id.get(t._1)
      val objId = resource_to_id.get(t._3)
      if(subjId != null && objId != null) {
        val sameSubjects = id_to_resource.get(subjId.get)
        val sameObjects = id_to_resource.get(objId.get)
        for(s <- sameSubjects.get)
          for(o <- sameObjects.get)
            println((s, t._2, o))
      }
    })

    sc.stop()
  }


}
