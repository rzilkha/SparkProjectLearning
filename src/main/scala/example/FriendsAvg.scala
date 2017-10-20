package example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Roee Zilkha on 10/20/2017.
  */
object FriendsAvg extends App{
  val conf = new SparkConf().setAppName("example")
  val sc = new SparkContext(conf);
  val rdd = sc.textFile("friends.csv")
  val ageCounts = rdd.map(line => {
      val arr = line.split(",")
      val age = arr(2)
      val numOfFriends = arr(3)
      (age ,numOfFriends.toInt)})

  val ageSumFriends = ageCounts.mapValues(friendsCount => (friendsCount,1))
    .reduceByKey((pair1,pair2) => (pair1._1+pair2._1,pair1._2+pair2._2))
//
   val avgByAge = ageSumFriends.mapValues(pair => pair._1/pair._2 ).saveAsTextFile("result.csv")



}
