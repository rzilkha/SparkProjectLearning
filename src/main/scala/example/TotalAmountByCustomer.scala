package example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Roee Zilkha on 10/21/2017.
  */
object TotalAmountByCustomer {

  def parseLine(line: String): Tuple2[Int, Double] = {
    val arr = line.split(",")
    val customerId = arr(0).toInt
    val amount = arr(2).toDouble
    return (customerId, amount)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("example")
    val sc = new SparkContext(conf);
    val rdd = sc.textFile("customer-orders.csv").map(parseLine)
      .reduceByKey((price1, price2) => price1 + price2).sortByKey();

    val results = rdd.collect();

    for ((key, value) <- results) {
      println(s"id:$key"+ f"amount:$value%1.2f")
    }
  }

}
