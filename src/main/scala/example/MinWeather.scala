package example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Roee Zilkha on 10/21/2017.
  */
object MinWeather {


  def parseData(line: String): Tuple2[String, Tuple2[String, Double]] = {
    val arr = line.split(",")
    val id = arr(0)
    val maxMin = arr(2)
    val temp = arr(3).toFloat*0.1*(0.9/0.5)+32.0
    return (id, (maxMin, temp))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("example")
    val sc = new SparkContext(conf);
    val rdd = sc.textFile("1800.csv").map(line => parseData(line))
      .filter { case (_, (maxorMin, _)) => maxorMin == "TMIN" }
      .map { case (station, (_, temp)) => (station, temp) }
      .reduceByKey((temp1, temp2) => Math.min(temp1, temp2))
      .saveAsTextFile("temp")

  }


}
