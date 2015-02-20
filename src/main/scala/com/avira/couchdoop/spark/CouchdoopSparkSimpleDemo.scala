package com.avira.couchdoop.spark

import com.avira.couchdoop.CouchbaseArgs
import com.avira.couchdoop.exp.{CouchbaseAction, CouchbaseOperation, CouchbaseOutputFormat}
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object CouchdoopSparkSimpleDemo {

  def main(args: Array[String]) {
    val inputFile = args(0)
    val urls = args(1)
    val bucket = args(2)
    val password = args(3)

    val conf = new SparkConf().setAppName("couchdoop-spark simple demo")
    val sc = new SparkContext(conf)

    val allLines = sc.textFile(inputFile)

    val cbOutput = allLines.map { line =>
      (line.hashCode.toString, new CouchbaseAction(CouchbaseOperation.SET, line))
    }

    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set(CouchbaseArgs.ARG_COUCHBASE_URLS.getPropertyName, urls)
    hadoopConf.set(CouchbaseArgs.ARG_COUCHBASE_BUCKET.getPropertyName, bucket)
    hadoopConf.set(CouchbaseArgs.ARG_COUCHBASE_PASSWORD.getPropertyName, password)

    cbOutput.saveAsNewAPIHadoopFile("tests/bogus_02", classOf[String], classOf[CouchbaseAction],
        classOf[CouchbaseOutputFormat], hadoopConf)
//    cbOutput.saveAsTextFile("tests/cs_out_01")

    sc.stop()
  }
}
