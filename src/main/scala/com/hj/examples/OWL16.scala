package com.hj.examples

import org.apache.spark.{SparkConf, SparkContext}

object OWL16 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("OWL16")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/OWL16.in")

    val triples = lines.map(t => {
      val arr = t.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
v owl:allValuesFrom w
v owl:onProperty p
u rdf:type v
u p x
=>
x rdf:type w
     */

    val allValSO = triples.filter(t => t._2.equals("owl:allValuesFrom")).map(t => (t._1, t._3)) //(v,w)
    val onPropSO = triples.filter(t => t._2.equals("owl:onProperty")).map(t => (t._1, t._3)) //(v,p)
    val types = triples.filter(t => t._2.equals("rdf:type")).map(t => (t._1, t._3)) //(u,v)

    val joinSchema = allValSO.join(onPropSO) //(v,(w,p))
    val joinSchemaP = joinSchema.map(t => (t._2._2, null)) //(p,null)

    val reverseTypes = types.map(t => (t._2, t._1)) //(v,u)
    val types_join_schema = reverseTypes.join(joinSchema).map(t => ((t._2._1, t._2._2._2), t._2._2._1)) //(v,(u,(w,p)))=>((u,p),w)
    val pso = triples.map(t => (t._2, (t._1, t._3))) //(p,(u,x))
    val spo_join_schema = pso.join(joinSchemaP).map(t => ((t._2._1._1, t._1), t._2._1._2)) //(p,((u,x),null))=>((u,p),x)

    val result = spo_join_schema.join(types_join_schema).map(t => t._2)

    result.foreach(t => println(t))
  }
}
