package example

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by Roee Zilkha on 11/4/2017.
  */
object MovieSimilarities {


  def loadMovieNames(): Map[Int, String] = {
    var lookup = Map[Int, String]()
    val lines = Source.fromFile("u.item", "iso-8859-1").getLines
    for (line <- lines) {
      val arr = line.split("\\|")
      lookup += (arr(0).toInt -> arr(1))
    }

    return lookup;
  }

  def parse(line: String): Tuple2[String, Tuple2[Int, Int]] = {
    val arr = line.split("\t")
    val user = arr(0)
    val movie = arr(1).toInt
    var rating = arr(2).toInt
    return (user, (movie, rating))
  }

  def filterDups(ratingsPair: Tuple2[String, Tuple2[Tuple2[Int, Int], Tuple2[Int, Int]]]): Boolean = {
    val (movie1, movie2) = ratingsPair._2
    return movie1._1 < movie2._1
  }

  def createMoviePairs(record: Tuple2[String, Tuple2[Tuple2[Int, Int], Tuple2[Int, Int]]]): Tuple2[Tuple2[Int, Int], Tuple2[Int, Int]] = {
    val (user, ((m1, r1), (m2, r2))) = record
    return ((m1, m2), (r1, r2))
  }


  def filterLowScore(moviePairScore: Tuple2[Tuple2[Int, Int], Tuple2[Double, Int]], movie: Int, minScore: Double, minCount: Int): Boolean = {
    val ((m1, m2), (score, count)) = moviePairScore
    return score > minScore && count > minCount && (m1 == movie || m2 == movie)
  }

  def calcMovieDifferance(ratings: Iterable[Tuple2[Int, Int]]): Tuple2[Double, Int] = {

    var numPairs = 0
    var sum_xx = 0
    var sum_yy = 0
    var sum_xy = 0
    for ((ratingX, ratingY) <- ratings) {
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator = sum_xy
    val denominator = Math.sqrt(sum_xx) * Math.sqrt(sum_yy)

    var score = 0.0
    if (denominator > 0) {
      score = (numerator / denominator.toDouble)
    }

    return (score, numPairs)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("example")
    val sc = new SparkContext(conf);

    val scoreThreshold = 0.97
    val apperancesOfPair = 50
    val movieId = 50

    val lookup = loadMovieNames()
    val rdd = sc.textFile("u.data").map(parse);

    // (m1,m2): (r1,r2),(r3,r4) ...(rn,rm)
    val moviePairsRatings = rdd.join(rdd).
      filter(filterDups).map(createMoviePairs).groupByKey()
    val moviePairComputed = moviePairsRatings.mapValues(calcMovieDifferance)
    val relevantMoviePairComputed = moviePairComputed.filter(pair => filterLowScore(pair, movieId, scoreThreshold, apperancesOfPair))
    val scoreMoviesPair = relevantMoviePairComputed.map(pairMovie => (pairMovie._2, pairMovie._1))
    val topTen = scoreMoviesPair.sortByKey(ascending = false).take(10)
    for (((score, count), (m1, m2)) <- topTen) {
      var similarMovie = m1
      if (m1 == movieId) {
        similarMovie = m2
      }
      val similarMovieName = lookup.get(similarMovie)
      println(s"movie $similarMovieName is similar, with strength $score and count $count")
    }
  }
}
