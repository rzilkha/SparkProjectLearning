package example

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.ListMap



/**
  * Created by Roee Zilkha on 10/14/2017.
  */
object RatingExample extends App{
  val conf = new SparkConf().setAppName("example")
  val sc = new SparkContext(conf);

  var rdd = sc.textFile("C:\\sparkProject\\src\\main\\resources\\u.data");

  // get by stars recommendation
  rdd = rdd.map(item => item.split("\t")(2))
  var result = rdd.countByValue();
  var sortedList = ListMap[String,Long](result.toSeq.sortWith(_._1 < _._1):_*)
  for ((k,v) <- sortedList)
    printf("key: %s, value: %s\n", k, v)

  val count = rdd.count()
  val javaExample = new JavaExample();
  println(s"mix java &scala try $javaExample.try123")
  println(s"the  result is $count")
}
