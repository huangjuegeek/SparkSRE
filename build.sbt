name := "SparkSRE"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.1" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.1"
    
