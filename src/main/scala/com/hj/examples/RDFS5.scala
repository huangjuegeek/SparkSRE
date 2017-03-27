package com.hj.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDFS5 {
  def transitive(rdd:RDD[(String, String)]) = {
    var rddTuple = rdd
    val reverseTuple = rddTuple.map(x => (x._2, x._1))

    var cur = 0L
    var pre = rddTuple.count
    var flag = true
    while (flag) {
      val joined = reverseTuple.join(rddTuple)
      val res = joined.map(x => x._2)
      rddTuple = rddTuple.union(res).distinct
      cur = rddTuple.count
      if(pre == cur) flag = false
      pre = cur
    }
    rddTuple
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 2) {
      System.out.println("Arguments are invalid! \nExample: <input_path> <output_path>")
      System.exit(1)
    }
    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName("RDFS5").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(inputPath)

    val triples = lines.map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
p rdfs:subPropertyOf q
q rdfs:subPropertyOf r
=>
p rdfs:subPropertyOf r
     */

    var subProp = triples.filter(x => x._2.equals("rdfs:subPropertyOf")).map(x => (x._1, x._3))
    subProp = transitive(subProp)

    subProp.foreach(x => println(x))
    subProp.saveAsTextFile(outputPath)
  }
}
