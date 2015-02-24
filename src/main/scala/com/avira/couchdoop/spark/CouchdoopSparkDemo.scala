package com.avira.couchdoop.spark

import com.avira.couchdoop.exp.CouchbaseAction
import com.avira.couchdoop.spark.CouchdoopSpark._
import org.apache.spark.rdd.RDD
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
    val urls = args(1).split(",").toSeq
    val bucket = args(2)
    val password = args(3)

    val conf = new SparkConf().setAppName("couchdoop-spark-demo")
    val sc = new SparkContext(conf)
    implicit val cbOutputConf = CouchdoopExportConf(urls, bucket, password)

    val allLines = sc.textFile(inputFile)

    // Create an index from the first word of lines to a list of lines with that word.
    val index = indexLines(allLines)

    // Prepare output for CouchbaseOutputFormat.
    val cbOutput = index.map {
      case (key, lines) => (key, CouchbaseAction.createSetAction(lines))
    }

    // Save to Couchbase with CouchbaseOutputFormat.
    cbOutput.saveToCouchbase

    sc.stop()
  }

  /**
   * Create an index from the first word of lines to a list of lines with that word.
   * @param allLines lines to index
   * @return
   */
  def indexLines(allLines: RDD[String]): RDD[(String, String)] = {
    allLines.flatMap { line =>
      val words = line.split("""[\s,.?;:'"()!]+""")
      if (words.nonEmpty && words(0).nonEmpty)
        Some((words(0), line))
      else
        None
    }.groupByKey().map {
      case (firstWord, lines) =>
        val escapedLines = lines.map(_.replaceAll("\"", """\\""""))
        val linesJson = escapedLines.mkString("[\"", "\",\"", "\"]")
        (firstWord, linesJson)
    }
  }
}
