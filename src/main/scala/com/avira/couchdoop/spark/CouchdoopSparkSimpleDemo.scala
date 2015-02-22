package com.avira.couchdoop.spark

import com.avira.couchdoop.exp.CouchbaseAction
import com.avira.couchdoop.spark.CouchdoopSpark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Write 3 hard-coded documents to Couchbase by using Hadoop CouchbaseOutputFormat.
 */
object CouchdoopSparkSimpleDemo {

  def main(args: Array[String]) {
    val urls = args(0).split(",").toSeq
    val bucket = args(1)
    val password = args(2)

    val conf = new SparkConf().setAppName("couchdoop-spark simple demo")
    val sc = new SparkContext(conf)
    implicit val cbOutputConf = CouchbaseOutputConf(urls, bucket, password)

    // Create 3 hard-coded documents.
    val cbOutput = sc.parallelize(
      Seq(
        ("x", CouchbaseAction.createSetAction("1")),
        ("y", CouchbaseAction.createAddAction("2")),
        ("z", CouchbaseAction.createDeleteAction)
      )
    )

    // Save to Couchbase with CouchbaseOutputFormat.
    cbOutput.saveToCouchbase

    sc.stop()
  }
}
