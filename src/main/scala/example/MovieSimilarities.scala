package example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Roee Zilkha on 11/4/2017.
  */
object MovieSimilarities {

  def parse(line: String): Tuple2[String, Tuple2[Int, Double]] = {
    val arr = line.split("\\|")
    val user = arr(0)
    val movie = arr(1).toInt
    var rating = arr(2).toDouble
    return (user, (movie, rating))
  }

  def filterDups(ratingsPair: Tuple2[String, Tuple2[Tuple2[Int, Double], Tuple2[Int, Double]]]): Boolean = {
    val (movie1, movie2) = ratingsPair._2
    return movie1._1 < movie2._1
  }

  def createMoviePairs(record: Tuple2[String, Tuple2[Tuple2[Int, Double], Tuple2[Int, Double]]]): Tuple2[Tuple2[Int, Int], Tuple2[Double, Double]] = {
    val (user, ((m1, r1), (m2, r2))) = record
    return ((m1, m2), (r1, r2))
  }


  def filterLowScore(moviePairScore: Tuple2[Tuple2[Int, Int], Tuple2[Double, Int]], movie: Int, minScore: Double, minCount: Int): Boolean = {
    val ((m1, m2), (score, count)) = moviePairScore
    return score > minScore && count > minCount && (m1 == movie || m2 == movie)
  }

  def calcMovieDifferance(ratings: Iterable[Tuple2[Double, Double]]): Tuple2[Double, Int] = {

    var sum_xx = 0
    var sum_yy = 0
    var sum_xy = 0

    var count = 0
    for ((ratingX, ratingY) <- ratings) {
      sum_xx += ratingX.toInt * ratingX.toInt
      sum_yy += ratingY.toInt * ratingY.toInt
      sum_xy += ratingX.toInt * ratingY.toInt
      count += 1
    }


    val numerator = sum_xy
    val denominator = Math.sqrt(sum_xx) * Math.sqrt(sum_yy)

    var score = 0.0
    if (denominator > 0) {
      score = (numerator.toDouble / denominator.toDouble)
    }

    return (score, count)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("example")
    val sc = new SparkContext(conf);

    val scoreThreshold = 0.97
    val apperancesOfPair = 50
    val movieId = 50


    val rdd = sc.textFile("u.data").map(parse);

    // (m1,m2): (r1,r2),(r3,r4) ...(rn,rm)
    val moviePairsRatings = rdd.join(rdd).
      filter(filterDups).map(createMoviePairs).groupByKey()

    val moviePairComputed = moviePairsRatings.mapValues(calcMovieDifferance)

    val relevantMoviePairComputed = moviePairComputed.filter(pair => filterLowScore(pair, movieId, scoreThreshold, apperancesOfPair))

    val scoreMoviesPair = moviePairComputed.map(pairMovie => (pairMovie._2, pairMovie._1))
    val topTen = scoreMoviesPair.sortByKey(ascending = false).take(10)


    print("top 10 similar movie for ")

    for ( ((score,count),(m1,m2))<-topTen){
        var similarMovie = m1
        if(m1==movieId){
           similarMovie = m2
        }
        print(s"movie $similarMovie is similar, with strength $score and count $count")
    }
  }
}
