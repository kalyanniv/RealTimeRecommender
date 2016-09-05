package com.veloo.kalyanni

import java.io.File
import scala.io.Source

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

/**
  * Created by Kalyanni Veloo on 19/7/2016.
  */
object RealTimeRecommender {
  def main(args:Array[String]) : Unit = {

    // wasb[s]://<containername>@<accountname>.blob.core.windows.net/<path>
    // Azure Blob Container that contains both movies.dat and ratings.dat
    val movieLensHomeDir = "wasbs://movielens@kalyannivsparkstore.blob.core.windows.net/"

    // Create SparkConf object
    // Create SparkContext object
    val conf = new SparkConf().setAppName("RealTimeRecommender")
    val sc = new SparkContext(conf)

    // Creates a movies RDD that contains (Int, String) in the form of (movieId, movieName) pairs
    // Collect the movie IDs, titles and map MovieID -> Movie Title
    val movies = sc.textFile(movieLensHomeDir + "movies.dat").map { line =>
      val fields = line.split("::")
      // format: (movieId, movieName)
      (fields(0).toInt, fields(1))
    }.collect.toMap

    // Creates a ratings RDD that contains (Int, Rating) pairs in the form of (timestamp % 10, Rating(userId, movieId, rating))
    // The last digit of the timestamp is a random key
    // The Rating class is a wrapper around the tuple (user: Int, product: Int, rating: Double)
    val ratings = sc.textFile(movieLensHomeDir + "ratings.dat").map { line =>
      val fields = line.split("::")
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    // Count the number of ratings, users and movies
    val numRatings = ratings.count
    val numUsers = ratings.map(_._2.user).distinct.count
    val numMovies = ratings.map(_._2.product).distinct.count

    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numMovies + " movies.")

    // Split the data into 3 subsets: Training(60%), validation(80%),Test(20%)
    val training = ratings.filter(x => x._1 < 6)
      .values
      .cache()
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .values
      .cache()
    val test = ratings.filter(x => x._1 >= 8)
      .values
      .cache()

    // Count the amount for each subset
    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

    // The ALS algorithm requires three parameters: matrix factors rank, number of iteration, and lambda
    // Select different values for ALS parameters and measure the RMSE for each combination to select the best combination:
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    // evaluate the best model on the test set
    val testRmse = computeRmse(bestModel.get, test, numTest)

    println("The best model was trained with rank = " + bestRank
      + " and lambda = " + bestLambda + ", and numIter = " + bestNumIter
      + ", and its RMSE on the test set is " + testRmse + ".")

    // Compare the best model with a naive baseline model based on average ratings
    val meanRating = training.union(validation).map(_.rating).mean
    val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

    // Make personal recommendations for users
    // Pass the first argument as a parameter
    val candidates = sc.parallelize(movies.keys.toSeq)
    val recommendations = bestModel.get
      .predict(candidates.map((args(0).toInt, _)))
      .collect()
      .sortBy(- _.rating)
      .take(10)

    var i = 1
    println("Movies recommended for user " + args(0).toInt + ":")
    recommendations.foreach { r =>
      println("%2d".format(i) + ": " + movies(r.product))
      i += 1
    }
  }

  // Define the RMSE function to evaluate the performance of the model
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }
}
