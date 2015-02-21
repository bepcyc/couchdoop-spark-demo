package com.avira.couchdoop.spark

import com.avira.couchdoop.CouchbaseArgs
import com.avira.couchdoop.exp.{CouchbaseAction, CouchbaseOutputFormat}
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/*
 WARN TaskSetManager: Lost task 1.0 in stage 0.0 (TID 1, hadoop-worker-02.sl1.avira.org): java.lang.LinkageError: loader (instance of  org/apache/spark/executor/ChildExecutorURLClassLoader$userClassLoader$): attempted  duplicate class definition for name: "com/avira/couchdoop/exp/CouchbaseOperation"
 https://github.com/apache/spark/pull/3725#discussion_r22126116
 */

object CouchdoopSparkSimpleDemo2 {

  def main(args: Array[String]) {
    val urls = args(0)
    val bucket = args(1)
    val password = args(2)

    val conf = new SparkConf().setAppName("couchdoop-spark simple demo 2")
    val sc = new SparkContext(conf)

    val cbOutput = sc.parallelize(
      Seq(
        ("x", CouchbaseAction.createSetAction("1")),
        ("y", CouchbaseAction.createSetAction("2")),
        ("z", CouchbaseAction.createSetAction("3"))
      )
    )

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
