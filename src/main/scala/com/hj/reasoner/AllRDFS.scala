package com.hj.reasoner

import com.hj.constant.Const
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object AllRDFS {
  def main(args: Array[String]): Unit = {
    if(args.length != 3) {
      System.out.println("Arguments are invalid! \nCorrect example: <input_path> <output_path> <parallelism>")
      System.exit(1)
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val parallelism = args(2).toInt

    val conf = new SparkConf().setAppName("SparkSRE RDFS reasoning")//.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val triples = sc.textFile(inputPath).map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1), arr(2))
    })

    val subClass = triples.filter(x => x._2.equals(Const.RDFS_SUBCLASS_OF))
    val subProp = triples.filter(x => x._2.equals(Const.RDFS_SUBPROPERTY_OF))
    val domain = triples.filter(x => x._2.equals(Const.RDFS_DOMAIN))
    val range = triples.filter(x => x._2.equals(Const.RDFS_RANGE))

    val funcProp = triples.filter(x => x._3.equals(Const.OWL_FUNCTIONAL_PROPERTY))
      .filter(x => x._2.equals(Const.RDF_TYPE))
    val invFuncProp = triples.filter(x => x._3.equals(Const.OWL_INVERSE_FUNCTIONAL_PROPERTY))
      .filter(x => x._2.equals(Const.RDF_TYPE))
    val symProp = triples.filter(x => x._3.equals(Const.OWL_SYMMETRIC_PROPERTY))
      .filter(x => x._2.equals(Const.RDF_TYPE))
    val transProp = triples.filter(x => x._3.equals(Const.OWL_TRANSITIVE_PROPERTY))
      .filter(x => x._2.equals(Const.RDF_TYPE))
    val inverseOf = triples.filter(x => x._2.equals(Const.OWL_INVERSE_OF))
    val hasValue = triples.filter(x => x._2.equals(Const.OWL_HAS_VALUE))
    val onProp = triples.filter(x => x._2.equals(Const.OWL_ON_PROPERTY))
    val someValuesFrom = triples.filter(x => x._2.equals(Const.OWL_SOME_VALUES_FROM))
    val allValuesFrom = triples.filter(x => x._2.equals(Const.OWL_ALL_VALUES_FROM))

    val schema = subClass.union(subProp).union(domain).union(range).union(funcProp)
      .union(invFuncProp).union(symProp).union(transProp).union(inverseOf).union(hasValue)
      .union(onProp).union(someValuesFrom).union(allValuesFrom)
    var instances = triples.subtract(schema)

    var subClass2 = subClass.map(x => (x._1, x._3))
    var subProp2 = subProp.map(x => (x._1, x._3))
    val domain2 = domain.map(x => (x._1, x._3))
    val range2 = range.map(x => (x._1, x._3))

    //RDFS 11
    subClass2 = transitive(subClass2).repartition(parallelism)

    //RDFS 5
    subProp2 = transitive(subProp2).repartition(parallelism)

    //RDFS 7
    var pso = instances.map(x => (x._2, (x._1, x._3)))
    val joined7 = pso.join(subProp2)
    val rdfs7Res = joined7.map(x => (x._2._1._1, x._2._2, x._2._1._2))

    instances = rdfs7Res.union(instances)

    //RDFS 3
    pso = instances.map(x => (x._2, (x._1, x._3)))
    val joined3 = pso.join(range2)
    val rdfs3Res = joined3.map(x => (x._2._1._2, x._2._2))

    //RDFS 2
    val joined2 = pso.join(domain2)
    val rdfs2Res = joined2.map(x => (x._2._1._1, x._2._2))

    //rdf:type
    var types = instances.filter(x => x._2.equals(Const.RDF_TYPE)).map(x => (x._1, x._3))
    types = rdfs3Res.union(rdfs2Res).union(types)

    //RDFS 9
    val reverseTypes = types.map(x => (x._2, x._1))
    val joined9 = subClass2.join(reverseTypes)
    val rdfs9Res = joined9.map(x => (x._2._2, x._2._1))

    //Generate reasoning results
    val outputInstances = rdfs2Res.union(rdfs3Res).union(rdfs9Res)
      .map(x =>(x._1, Const.RDF_TYPE, x._2)).union(rdfs7Res).distinct(parallelism)
    outputInstances.saveAsTextFile(outputPath + "/instance")
    val outputSchema = schema
      .union(subClass2.map(x => (x._1, Const.RDFS_SUBCLASS_OF, x._2)))
      .union(subProp2.map(x => (x._1, Const.RDFS_SUBPROPERTY_OF, x._2)))
      .repartition(parallelism).distinct(parallelism)
    outputSchema.saveAsTextFile(outputPath + "/schema")
  }

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
}
