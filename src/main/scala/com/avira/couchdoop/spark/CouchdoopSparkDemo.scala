package com.avira.couchdoop.spark

import com.avira.couchdoop.exp.{CouchbaseOutputFormat, CouchbaseAction}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Read a text file from HDFS and index each line in Couchbase by its first word by using
 * CouchbaseOutputFormat.
 *
 * Run it with --conf "spark.files.userClassPathFirst=false" (default) because of Bug SPARK-4877
 * in Spark 1.2.x.
 */
object CouchdoopSparkDemo {

  def main(args: Array[String]) {
    val inputFile = args(0)
    val urls = args(1)
    val bucket = args(2)
    val password = args(3)

    val conf = new SparkConf().setAppName("couchdoop-spark-demo")
    val sc = new SparkContext(conf)

    // Configure Hadoop CouchbaseOutputFormat.
    val hadoopJob = Job.getInstance(sc.hadoopConfiguration)
    CouchbaseOutputFormat.initJob(hadoopJob, urls, bucket, password)
    val hadoopConf = hadoopJob.getConfiguration

    val allLines = sc.textFile(inputFile)

    // Create an index from the first word of lines to a list of lines with that word.
    val index = allLines.flatMap { line =>
      val words = line.split("""[\s,.?;:'"()!]+""")
      if (words.length >= 1 && !words(0).isEmpty)
        Some((words(0), line))
      else
        None
    }.groupByKey()

    // Prepare output for CouchbaseOutputFormat.
    val cbOutput = index.map {
      case (firstWord, lines) =>
        val escapedLines = lines.map(_.replaceAll("\"", """\\""""))
        val linesJson = escapedLines.mkString("[\"", "\",\"", "\"]")
        (firstWord, CouchbaseAction.createSetAction(linesJson))
    }

    // Save to Couchbase with CouchbaseOutputFormat.
    cbOutput.saveAsNewAPIHadoopDataset(hadoopConf)

    sc.stop()
  }
}
