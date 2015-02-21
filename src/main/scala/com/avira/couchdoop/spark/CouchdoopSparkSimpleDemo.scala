package com.avira.couchdoop.spark

import com.avira.couchdoop.exp.{CouchbaseOutputFormat, CouchbaseAction}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Write 3 hard-coded documents to Couchbase by using Hadoop CouchbaseOutputFormat.
 */
object CouchdoopSparkSimpleDemo {

  def main(args: Array[String]) {
    val urls = args(0)
    val bucket = args(1)
    val password = args(2)

    val conf = new SparkConf().setAppName("couchdoop-spark simple demo 2")
    val sc = new SparkContext(conf)

    // Configure Hadoop CouchbaseOutputFormat.
    val hadoopJob = Job.getInstance(sc.hadoopConfiguration)
    CouchbaseOutputFormat.initJob(hadoopJob, urls, bucket, password)
    val hadoopConf = hadoopJob.getConfiguration

    // Create 3 hard-coded documents.
    val cbOutput = sc.parallelize(
      Seq(
        ("x", CouchbaseAction.createSetAction("1")),
        ("y", CouchbaseAction.createSetAction("2")),
        ("z", CouchbaseAction.createSetAction("3"))
      )
    )

    // Save to Couchbase with CouchbaseOutputFormat.
    cbOutput.saveAsNewAPIHadoopDataset(hadoopConf)

    sc.stop()
  }
}
