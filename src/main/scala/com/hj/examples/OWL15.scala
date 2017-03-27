package com.hj.examples

import org.apache.spark.{SparkConf, SparkContext}

object OWL15 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("OWL15")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("input/OWL15.in")

    val triples = lines.map(t => {
      val arr = t.split(" ")
      (arr(0), arr(1), arr(2))
    })

    /*
v owl:someValuesFrom w
v owl:onProperty p
x rdf:type w
u p x
=>
u rdf:type v
 */

    val someValSO = triples.filter(t => t._2.equals("owl:someValuesFrom")).map(t => (t._1, t._3))
    val onPropSO = triples.filter(t => t._2.equals("owl:onProperty")).map(t => (t._1, t._3))
    val types = triples.filter(t => t._2.equals("rdf:type")).map(t => (t._1, t._3)) //(x,w)

    val joinSchema = someValSO.join(onPropSO) //(v,(w,p))
    val joinSchemaW = joinSchema.map(t => (t._2._1, (t._2._2, t._1))) //(w,(p, v))
    val joinSchemaP = joinSchema.map(t => (t._2._2, null)) //(p, null)

    val reverseTypes = types.map(t => (t._2, t._1)) //(w,x)
    val types_join_schema = reverseTypes.join(joinSchemaW).map(t => ((t._2._1, t._2._2._1), t._2._2._2)) //((x,p),v)

    val pso = triples.map(t => (t._2, (t._1, t._3))) //(p,(u,x))
    val spo_join_schema = pso.join(joinSchemaP).map(t => ((t._2._1._2, t._1), t._2._1._1)) //((x,p),u)

    val result = spo_join_schema.join(types_join_schema).map(t => t._2) //(u,v)

    result.foreach(t => println(t))

    sc.stop()
  }
}
