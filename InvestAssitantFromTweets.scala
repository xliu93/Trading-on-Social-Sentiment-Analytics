/* CSCI-GA 3033 Big Data Application Development
 * Investment Assistant Based on News Feed
 * Submitted by:
 * - Xialiang Liu
 * - Dailing Zhu
 */

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import scala.util.{Success, Try}

// create a Spart SQL Context
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// Further development will read input from command line or UI.
val isSingleStock = true
if (isSingleStock) {
  val targetStock = "SP500"
}
else {
  val targetIndustry = "ENERGY"
}

// Load stock price data of the target Stock
val stockDataFile = "hdfs:///user/xl2053/InvestAssistant/yahoo_finance_dataset/" + targetStock + ".csv"
val stockPrice = sc.TextFile(stockDataFile)


// Load twitter data
var tweetDF = sqlContext.read.json("hdfs:///user/xl2053/InvestAssistant/twitter_dataset/*")
