package com.avira.couchdoop.spark

import com.avira.couchdoop.CouchbaseArgs
import com.avira.couchdoop.exp.{CouchbaseOutputFormat, CouchbaseOperation, CouchbaseAction}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

object CouchdoopSparkDemo {

  def main(args: Array[String]) {
    val inputFile = args(0)
    val urls = args(1)
    val bucket = args(2)
    val password = args(3)

    val conf = new SparkConf().setAppName("couchdoop-spark-demo")
    val sc = new SparkContext(conf)

    val allLines = sc.textFile(inputFile)

    // Create an index from the first word of lines to a list of lines with that word.
    val index = allLines.flatMap { line =>
      val words = line.split("""[\s,.?;:'"()!]+""")
      if (words.length >= 1)
        Some((words(0), line))
      else
        None
    }.groupByKey()

    val cbOutput = index.map {
      case (firstWord, lines) =>
        val escapedLines = lines.map(_.replaceAll("\"", """\\""""))
        val linesJson = escapedLines.mkString("[\"", "\",\"", "\"]")
        (firstWord, new CouchbaseAction(CouchbaseOperation.SET, linesJson))
    }

    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set(CouchbaseArgs.ARG_COUCHBASE_URLS.getPropertyName, urls)
    hadoopConf.set(CouchbaseArgs.ARG_COUCHBASE_BUCKET.getPropertyName, bucket)
    hadoopConf.set(CouchbaseArgs.ARG_COUCHBASE_PASSWORD.getPropertyName, password)

    cbOutput.saveAsNewAPIHadoopFile("tests/bogus_01", classOf[String], classOf[CouchbaseAction],
        classOf[CouchbaseOutputFormat], hadoopConf)
//    cbOutput.saveAsTextFile("tests/cs_out_01")

    sc.stop()
  }
}
