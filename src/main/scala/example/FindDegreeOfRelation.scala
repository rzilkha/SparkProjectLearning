package example

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._

import scala.collection.mutable.ListBuffer

/**
  * Created by Roee Zilkha on 10/28/2017.
  */
object FindDegreeOfRelation extends App {

  def src:Int = 5306
  def dst:Int = 14

  var hitCount: LongAccumulator = null

  def initialParse(line: String): Tuple2[Int, Tuple3[List[Int], String, Int]] = {
    val arr = line.split(" ")
    val characterId = arr(0).toInt;
    val friends = arr.slice(1, arr.length).map(number => number.toInt).toList

    var color = "WHITE"
    var distance = 99999
    if (characterId == src) {
      color = "GRAY"
      distance = 0
    }

    return (characterId, (friends, color, distance))
  }


  def bfsMap(record: (Int, (List[Int], String, Int))): List[(Int, (List[Int], String, Int))] = {
    val characterId = record._1
    val (friends, color, distance) = record._2
    var listBuf = ListBuffer[Tuple2[Int, Tuple3[List[Int], String, Int]]]()
    var newColor = color

    if (color == "GRAY") {
      for (friend <- friends) {
        if (friend == dst) {
          hitCount.add(1)
        }

        val newEntry = (friend, (List[Int](), "GRAY", distance + 1))
        listBuf += newEntry
      }
      newColor = "BLACK"
    }

    val originalRecord = (characterId, (friends, newColor, distance))
    listBuf += originalRecord

    return listBuf.toList

  }

  def bfsReduce(record1: (List[Int], String, Int), record2: (List[Int], String, Int)): (List[Int], String, Int) = {
    val (friends1:List[Int], color1, distance1) = record1
    val (friends2, color2, distance2) = record2

    var friends = List[Int]()

    if (friends1.size > 0) {
      friends = friends++friends1
    }
    if (friends2.size > 0) {
      friends = friends++friends2
    }

    var color = "WHITE"
    if ((color1 == "BLACK") || (color2 == "BLACK")) {
      color = "BLACK"
    } else if ((color1 == "GRAY") || (color2 == "GRAY")) {
      color = "GRAY"
    }

    var distance = Math.min(distance1, distance2)

    return (friends, color, distance)

  }

  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("example")
    val sc = new SparkContext(conf)
    hitCount = sc.longAccumulator("found");

    var iterationRdd = sc.textFile("Marvel-Graph.txt").map(initialParse)
   // iterationRdd.saveAsTextFile("te.txt")
    for (i <- 1 to 10) {
      val mapped = iterationRdd.flatMap(bfsMap)
      println("Processing " + mapped.count() + " values."+i)
      if (hitCount.value != 0) {
        println(s"found dst, from ${hitCount.value} direction")
        break
      }

      iterationRdd = mapped.reduceByKey(bfsReduce)
    }

  }
}
