package com.hj.reasoner

import com.hj.constant.Const
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object AllOWL {
  def main(args: Array[String]): Unit = {
    if(args.length != 3) {
      System.out.println("Arguments are invalid! \n" +
        "Correct example: <input_instance_path> <input_schema_path> <output_path> <parallelism>")
      System.exit(1)
    }
    val inputInstancePath = args(0)
    val inputSchemaPath = args(1)
    val outputPath = args(2)
    val parallelism = args(3).toInt

    val conf = new SparkConf().setAppName("SparkSRE OWL Horst reasoning")//.setMaster("local[2]")
    val sc = new SparkContext(conf)

    var instances = sc.textFile(inputInstancePath).map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1), arr(2))
    })

    val schema = sc.textFile(inputSchemaPath).map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1), arr(2))
    })

    var subClass2 = schema.filter(x => x._2.equals(Const.RDFS_SUBCLASS_OF)).map(x => (x._1, x._3))
    var subProp2 = schema.filter(x => x._2.equals(Const.RDFS_SUBPROPERTY_OF)).map(x => (x._1, x._3))
    val domain2 = schema.filter(x => x._2.equals(Const.RDFS_DOMAIN)).map(x => (x._1, x._3))
    val range2 = schema.filter(x => x._2.equals(Const.RDFS_RANGE)).map(x => (x._1, x._3))

    var triples = instances.filter(x => !(x._2.equals(Const.OWL_SAME_AS) || x._2.equals(Const.RDF_TYPE)))
    var types = instances.filter(x => x._2.equals(Const.RDF_TYPE)).map(x => (x._1,x._3))
    var sames = instances.filter(x => x._2.equals(Const.OWL_SAME_AS)).map(x => (x._1,x._3))

    val funcProp = schema.filter(x => x._3.equals(Const.OWL_FUNCTIONAL_PROPERTY))
      .filter(x => x._2.equals(Const.RDF_TYPE)).map(x => x._1)
    val bcFuncProp = sc.broadcast(funcProp.collect)

    val invFuncProp = schema.filter(x => x._3.equals(Const.OWL_INVERSE_FUNCTIONAL_PROPERTY))
      .filter(x => x._2.equals(Const.RDF_TYPE)).map(x => x._1)
    val bcInvFuncProp = sc.broadcast(invFuncProp.collect)

    val symProp = schema.filter(x => x._3.equals(Const.OWL_SYMMETRIC_PROPERTY))
      .filter(x => x._2.equals(Const.RDF_TYPE)).map(x => x._1)
    val bcSymProp = sc.broadcast(symProp.collect)

    val transProp = schema.filter(x => x._3.equals(Const.OWL_TRANSITIVE_PROPERTY))
      .filter(x => x._2.equals(Const.RDF_TYPE)).map(x => x._1)

    val inverse1 = schema.filter(x => x._2.equals(Const.OWL_INVERSE_OF)).map(x => (x._1,x._3))
    val inverse2 = inverse1.map(t => (t._2,t._1))
    val inverseOf = inverse1.union(inverse2)
    val bcInverseOf = sc.broadcast(inverseOf.collect.toMap)

    val hasValue = schema.filter(x => x._2.equals(Const.OWL_HAS_VALUE)).map(x => (x._1,x._3))
    val bcHasValue = sc.broadcast(hasValue.collect.toMap)

    val onProp = schema.filter(x => x._2.equals(Const.OWL_ON_PROPERTY)).map(x => (x._1,x._3))
    val bcOnProp = sc.broadcast(onProp.collect.toMap)

    val someValuesFrom = schema.filter(x => x._2.equals(Const.OWL_SOME_VALUES_FROM)).map(x => (x._1,x._3))
    val allValuesFrom = schema.filter(x => x._2.equals(Const.OWL_ALL_VALUES_FROM)).map(x => (x._1,x._3))

    val some_onprop = someValuesFrom
      .filter(x => bcOnProp.value.contains(x._1)).map(x => (x._2, (bcOnProp.value(x._1), x._1)))
    val bcSome_onprop = sc.broadcast(some_onprop.collect.toMap)  //(w,(p,v))
    val bcSome_onprop2 = sc.broadcast(bcSome_onprop.value.map(x => (x._2._1, null)))  //(p,null)

    val all_onprop = allValuesFrom
      .filter(x => bcOnProp.value.contains(x._1)).map(x => (x._1, (x._2, bcOnProp.value(x._1))))
    val bcAll_onprop = sc.broadcast(all_onprop.collect.toMap)  //(v,(w,p))
    val bcAll_onprop2 = sc.broadcast(bcAll_onprop.value.map(x => (x._2._2, null)))  //(p,null)

    //RDFS 11
    subClass2 = transitive(subClass2).repartition(parallelism)

    //RDFS 5
    subProp2 = transitive(subProp2).repartition(parallelism)

    var iteratorFlag = true
    do {
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

      //RDFS 9
      val reverseTypes = rdfs3Res.union(rdfs2Res).union(types).map(x => (x._2, x._1))
      val joined9 = subClass2.join(reverseTypes)
      val rdfs9Res = joined9.map(x => (x._2._2, x._2._1))

      //OWL 3
      val owl3Res = triples.filter(x => bcSymProp.value.contains(x._2)).map(x => (x._3, x._2, x._1))

      //OWL 8a 8b
      val owl8Res = triples.filter(x => bcInverseOf.value.contains(x._2))
        .map(x => (x._3, bcInverseOf.value(x._2), x._1))

      //OWL 14a
      val owl14aRes = triples
        .filter(x => bcHasValue.value.contains(x._3) && bcOnProp.value.contains(x._3) && bcOnProp.value(x._3).equals(x._2))
        .map(x => (x._1, x._3))

      //OWL 14b
      val owl14bRes = types
        .filter(x => bcHasValue.value.contains(x._2) && bcOnProp.value.contains(x._2))
        .map(x => (x._1, bcOnProp.value(x._2), x._2))

      val incTriples = rdfs7Res.union(owl14bRes).union(owl8Res).union(owl3Res)
        .distinct(parallelism).subtract(triples, parallelism)
      val incTypes = rdfs2Res.union(rdfs3Res).union(owl14aRes)
        .distinct(parallelism).subtract(types, parallelism)
      val countIncTriples = incTriples.count()
      val countIncTypes = incTypes.count()

      types = types.union(incTypes)
      triples = triples.union(incTriples)

      if(countIncTypes != 0) {
        //OWL 15
        val types15 = types
          .filter(x => bcSome_onprop.value.contains(x._2))
          .map(x => ((x._1, bcSome_onprop.value(x._2)._1), bcSome_onprop.value(x._2)._2))
        val triples15 = triples
          .filter(x => bcSome_onprop2.value.contains(x._2))
          .map(x => ((x._3, x._2), x._1))
        val owl15Res = triples15.join(types15).map(x => x._2)
        types = types.union(owl15Res)

        //OWL 16
        val types16 = types
          .filter(x => bcAll_onprop.value.contains(x._2))
          .map(x => ((x._1, bcAll_onprop.value(x._2)._2), bcAll_onprop.value(x._2)._1))
        val triples16 = triples
          .filter(x => bcAll_onprop2.value.contains(x._2))
          .map(x => ((x._1, x._1), x._3))
        val owl16Res = triples16.join(types16).map(x => x._2)
        types = types.union(owl16Res)
      }

      if(countIncTriples == 0 && countIncTypes == 0)  iteratorFlag = false
    } while(iteratorFlag)

    //OWL 1
    val owl1Temp = triples.filter(x => bcFuncProp.value.contains(x._2)).map(x => ((x._1, x._2), x._3))
    val owl1Res = owl1Temp.join(owl1Temp).map(x => (x._2._1, x._2._2)).filter(x => !x._1.equals(x._2))

    //OWL 2
    val owl2Temp = triples.filter(x => bcInvFuncProp.value.contains(x._2)).map(x => ((x._2, x._3), x._1))
    val owl2Res = owl2Temp.join(owl2Temp).map(x => (x._2._1, x._2._2)).filter(x => !x._1.equals(x._2))

    sames = sames.union(owl1Res).union(owl2Res)
    val outputSames = sames.map(x => (x._1, Const.OWL_SAME_AS, x._2))
    val outputTypes = types.map(x => (x._1, Const.RDF_TYPE, x._2))
    val outputInstances = triples.union(outputTypes).union(outputSames).distinct(parallelism)
    val outputSchema = subClass2.map(x => (x._1, Const.RDFS_SUBCLASS_OF, x._2))
      .union(subProp2.map(x => (x._1, Const.RDFS_SUBPROPERTY_OF, x._2)))
      .union(domain2.map(x => (x._1, Const.RDFS_DOMAIN, x._2)))
      .union(range2.map(x => (x._1, Const.RDFS_RANGE, x._2)))
      .union(funcProp.map(x => (x, Const.RDF_TYPE, Const.OWL_FUNCTIONAL_PROPERTY)))
      .union(invFuncProp.map(x => (x, Const.RDF_TYPE, Const.OWL_FUNCTIONAL_PROPERTY)))
      .union(symProp.map(x => (x, Const.RDF_TYPE, Const.OWL_SYMMETRIC_PROPERTY)))
      .union(transProp.map(x => (x, Const.RDF_TYPE, Const.OWL_TRANSITIVE_PROPERTY)))
      .union(inverseOf.map(x => (x._1, Const.OWL_INVERSE_OF, x._2)))
      .union(hasValue.map(x => (x._1, Const.OWL_HAS_VALUE, x._2)))
      .union(onProp.map(x => (x._1, Const.OWL_ON_PROPERTY, x._2)))
      .union(someValuesFrom.map(x => (x._1, Const.OWL_SOME_VALUES_FROM, x._2)))
      .union(allValuesFrom.map(x => (x._1, Const.OWL_ALL_VALUES_FROM, x._2)))
      .repartition(1)

    outputSchema.saveAsTextFile(outputPath + "/schema")
    outputInstances.saveAsTextFile(outputPath + "/instance")
  }

  def transitive(rdd:RDD[(String, String)]) = {
    var rddTuple = rdd
    val reverseTuple = rddTuple.map(x => (x._2, x._1))

    var cur = 0L
    var pre = rddTuple.count()
    var flag = true
    while (flag) {
      val joined = reverseTuple.join(rddTuple)
      val res = joined.map(x => x._2)
      rddTuple = rddTuple.union(res).distinct
      cur = rddTuple.count()
      if(pre == cur) flag = false
      pre = cur
    }
    rddTuple
  }
}
