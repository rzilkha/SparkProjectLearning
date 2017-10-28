package example

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source



/**
  * Created by Roee Zilkha on 10/21/2017.
  */
object MaxMovieLen {

  def parseLine(line: String): Tuple2[String, Int] = {
    val arr = line.split("\t")
    val movieId = arr(1)
    return (movieId, 1)
  }

  def loadLookup(filename: String): Map[String, String] = {
    var lookup = Map[String, String]()
    val lines = Source.fromFile(filename, "iso-8859-1").getLines
    for (line <- lines) {
      val arr = line.split("\\|")
      lookup += (arr(0) -> arr(1))
    }

    return lookup;

  }

  def main(args: Array[String]): Unit = {
    val lookup = loadLookup("u.item")
        val conf = new SparkConf().setAppName("example")
    val sc = new SparkContext(conf);
    val lookupbrod = sc.broadcast(lookup)

    val rdd = sc.textFile("u.data").map(parseLine).map{case(id,count) => (lookupbrod.value.get(id).get,count)}
      .reduceByKey((n1, n2) => n1 + n2).map {
      case (id, appearances) => (appearances, id)
    }.sortByKey(ascending = false);

    rdd.saveAsTextFile("check.txt")
  }

}
