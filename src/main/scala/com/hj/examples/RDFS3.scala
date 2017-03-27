package com.hj.examples

import com.hj.constant.Const
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RDFS3 {
  def main(args: Array[String]): Unit = {
    //Arguments: input/RDFS3.in output/RDFS3.out
    if(args.length != 2) {
      System.out.println("Arguments are invalid! \nExample: <input_path> <output_path>")
      System.exit(1)
    }
    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName("RDFS3.in").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(inputPath)  //"input/RDFS3.in"

    val triples = lines.map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
p rdfs:range x
s p o
=>
o rdf:type x
     */

    val partitioner = new HashPartitioner(2)

    val range = triples.filter(x => x._2.equals(Const.RDFS_RANGE)).map(x => (x._1, x._3))
    val pso = triples.map(x => (x._2, (x._1, x._3))).partitionBy(partitioner)
    val joined = pso.join(range)
    val res = joined.map(x => (x._2._1._2, x._2._2))

    res.foreach(x => println(x))
    res.saveAsTextFile(outputPath)
  }
}
