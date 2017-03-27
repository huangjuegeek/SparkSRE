package com.hj.examples

import com.hj.constant.Const
import org.apache.spark.{SparkConf, SparkContext}

object RDFS2 {
  def main(args: Array[String]): Unit = {
    //Arguments: input/RDFS2.in output/RDFS.out
    if(args.length != 2) {
      System.out.println("Arguments are invalid! \nExample: <input_path> <output_path>")
      System.exit(1)
    }
    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName("RDFS2.in").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(inputPath) //input/RDFS2.in

    val triples = lines.map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
p rdfs:domain x
s p o
=>
s rdf:type x
     */

    val domain = triples.filter(x => x._2.equals(Const.RDFS_DOMAIN)).map(x => (x._1, x._3))
    val pso = triples.map(x => (x._2, (x._1, x._3)))
    val joined = pso.join(domain)
    val res = joined.map(x => (x._2._1._1, x._2._2))

    res.foreach(x => println(x))
  }
}
