/* 
 * CSCI-GA 3033 Big Data Application Development 
 * Project: Investment Assistant Based on Social Sentiment Analytics
 * Author:
 *    - Xialiang Liu (xl2053@nyu.edu)
 *    - Dailing Zhu (dz1104@nyu.edu)
 *
 * This is the main class of our application: InvestmentAssistant
 */

package invAssistant

// Include all functions from SentimentUtils
import invAssistant.SentimentUtils._
import invAssistant.GradientBoostingUtils._
import invAssistant.TimeSeriesUtils._

// Import Spark related packages 
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf

// Import Java-related packages
import java.util.Calendar
import java.io.File
import java.io.PrintWriter

// Import Scala-related packages
import scala.io.Source
//import scala.collection.mutable.ArrayBuffer
//import scala.util.{Success, Try}

object InvestmentAssistant {
  
  def write_log(file_writer: PrintWriter, text: String) {
    file_writer.write(text)
  }
  
  def main(args: Array[String]) {
    /* Make sure that we have enough parameters */
    if (args.length < 3) {
      System.err.println("Usage: InvestmentAssistant <path_to_input> <path_to_output> <ticker_name>")
      System.exit(1)
    }
   
    /* BEGIN */
    val sc = new SparkContext()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    import sqlContext._
    import sqlContext.implicits._
  
    /* Get user-defined input path and output path */
    val DATA_INPUT_PATH = args(0)
    val OUTPUT_PATH = args(1)
    val TARGET_TICKER = args(2)

    /* ============================
     * Set Hyperparameters.
     * ============================
     * These parameters control whether we do analysis on 
     * tweets and news related to a specific stock.
     */
    val MIN_TWEETS_DAILY = 5 // minimum number of tweets per day
    val MIN_TWEETS_DAYS = 100 // minimum number of days having tweets posted per year
    val MIN_NEWS_DAILY = 0.5 // minimum number of news related per day
    
    /* ============================
     * Data Loading.
     * ============================
     */
    // Loading data 1: Twitter feeds
    val twitter_raw = sc.textFile(DATA_INPUT_PATH + "twitter/*.csv")
    val twitter_split = twitter_raw.map(line => line.split(";", -1))
    
    val twitter_filt = twitter_split.filter(array =>
        array(1) != "" && array(1) != "date" && array(1).length == 16 &&
        array(2) != "" && array(3) != "" &&
        array(4) != "" && array(4) != null && array(4).length >2)
    
    /*
    val twitter_select = twitter_filt.map(array => Row(
        array(1).split(" ")(0),
        array(2).toInt,
        array(3).toInt,
        array(4).substring(1, array(4).length - 1)))
    
    val twitter_Schema = StructType(Array(
        StructField("date", StringType, false),
        StructField("retweets", IntegerType, false),
        StructField("favorites", IntegerType, false),
        StructField("text", StringType, false)))
    // this DF might not be in use later.
    // together with above schema and twitter_select
    var twitter_DF = sqlContext.createDataFrame(twitter_select, twitter_Schema)
    */
    val twitter_selectTuple = twitter_filt.map(array => (
        array(1).split(" ")(0),
        array(2).toInt, 
        array(3).toInt,
        array(4).substring(1, array(4).length-1 )))

    val twitter_senti = twitter_selectTuple.map( line => (
        line._1,
        line._2,
        line._3,
        detectSentiment(line._4),
        line._4))
    //twitter_senti.cache() // Remember to unpersist this rdd after query-search for Target-Ticker

    
    // Loading data 2: Reuters News (headline)
    val reuters_raw = sc.textFile(DATA_INPUT_PATH + "reuters/reuters.csv")
    val reuters_split = reuters_raw.map(
        line => line.split("\t", -1))

    val reuters_filt = reuters_split.filter(
      _.length == 4).map(
        array => (array(1), array(2))).filter(
          pair => 
            pair._1 != "" && pair._1.length > 8 &&
            pair._2 != "")

    val reuters_select = reuters_filt.map(pair => ( 
        pair._1.substring(1, 5)+"-"+pair._1.substring(5,7)+"-"+pair._1.substring(7,9),
        pair._2.substring(1, pair._2.length-1)))

    val reuters_senti = reuters_select.map( line => (
        line._1,
        detectSentiment(line._2),
        line._2))
    //reuters_senti.cache()

    /*
    println("\n======= Report 5 tweets: =======")
    twitter_senti.take(5).foreach(println)
    println("\n======= Report 5 headlines from Reuters: ========")
    reuters_senti.take(5).foreach(println)
    */

    // Loading data 3: Stock Dataset
    val ticker_raw = sc.textFile(DATA_INPUT_PATH + "stock_dataset/tickers/*.csv")
    val ticker_info = ticker_raw.map(_.split("\",\"")).map(array =>
        (array(0).substring(1, array(0).length), array(1))).filter(
          _._1 != "Symbol")

    val ticker_pair = ticker_info.filter(_._1 == TARGET_TICKER)
    val ticker_pair_count = ticker_pair.count
    if (ticker_pair_count == 0) {
      System.err.println("Invalid ticker input! Does not belong to NYSE/AMEX/NASDAQ!")
      System.exit(1)
    }
    else if (ticker_pair_count > 1){
      System.err.println("Something is wrong: more than one companies related to given ticker! Sorry!")
      System.exit(1)
    }
    val TARGET_COMPANY = ticker_pair.first._2
    

    // load price data
    val price_raw = sc.textFile(DATA_INPUT_PATH+"stock_dataset/"+TARGET_TICKER+".csv")
    //price_raw.cache() 
    // We need price_raw rdd for further computation.
    
    /*
    val price = price_raw.map(_.split(",")).filter(
      _(0) != "Date").map( array => (
        array(0),     //Date
        array(5).toDouble, // Adjusted Price
        array(6).toInt) // Trading Volume
    */

    // COMPUTE DAILY RETURN
    computeStockReturn(sqlContext, TARGET_TICKER, price_raw)
    //price_raw.unpersist()

    /* =======================================
     * Make Design Matrix for Tweets and News
     * =======================================
     */
    val tweets_for_ticker = twitter_senti.filter(line => 
        (line._5.contains(TARGET_TICKER)) || (line._5.contains(TARGET_COMPANY)))

    val news_for_ticker = reuters_senti.filter(line=>
        (line._3.contains(TARGET_TICKER)) || (line._3.contains(TARGET_COMPANY)))
    
    //tweets_for_ticker.cache()
    //news_for_ticker.cache()
    //twitter_senti.unpersist()
    //reuters_senti.unpersist()
    
    
    println("\n======= Report 5 tweets related to ", TARGET_TICKER, " =======")
    tweets_for_ticker.take(5).foreach(println)
    println("\n======= Report 5 headlines from Reuters related to " TARGET_TICKER," ========")
    news_for_ticker.take(5).foreach(println)
    // COMPUTE SUM & AVERAGE & DAY-COUNT


    // Clean up before terminate application.
    //logfile_writer.close()
    //tweets_for_ticker.unpersist()
    //news_for_ticker.unpersist()
    sc.stop()
  }
}
