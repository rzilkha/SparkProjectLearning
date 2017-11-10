package example

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

/**
  * Created by Roee Zilkha on 11/10/2017.
  */
object SqlToCassandra {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("example").set("spark.cassandra.connection.host","localhost")
    val sc = new SparkContext(conf)
    val data = sc.cassandraTable("kv","test").where("check='2'")
    val rows = data.collect()
    for (row<-rows){
        println(row)
    }

  }
}
