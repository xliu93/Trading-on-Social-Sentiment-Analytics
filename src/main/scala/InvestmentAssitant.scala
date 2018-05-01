import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
//import org.apache.spark.sql.SQLContext

//import sqlContext._
//import sqlContext.implicits._
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.DataFrameNaFunctions

//import org.apache.spark.sql.types._
//import org.apache.spark.sql.Row

object InvestmentAssistant {

  def main(args: Array[String]) {

    val sc = new SparkContext()
    //val sqlContext = new SQLContext(sc)
    //import sqlContext._
    //import sqlContext.implicits._
    
    
    /* ========================
     * Loading Twitter Dataset
     * ========================
     */
    // Load raw data
    val twitter_raw = sc.textFile("/user/xl2053/BDAD_project/twitter/*.csv")
    
    // Split each line using delimiter ";"
    val twitter_split = twitter_raw.map(line => line.split(";", -1))

    // Drop the records without timestamp.
    // And no empty entries for $retweets, #favorites, and text
    val twitter_filt = twitter_split.filter(array =>
          array(1) != "" && array(1) != "date" && array(1).length == 16 &&
          array(2) != "" && array(3) != "" && 
          array(4) != "" && array(4) != null && array(4).length > 2)

    // Select the columns we need:
    val twitter_select = twitter_filt.map(array => (
          array(1).split(" ")(0), 
          array(2).toInt, 
          array(3).toInt, 
          array(4).substring(1, array(4).length - 1)))

    /* A tweet record becomes: 
     * (  date: String, e.g. 2010-01-04
     *    #retweets: Int
     *    #favorites: Int
     *    text: String 
     *  )
     */

    val twitter_sortedGroup = twitter_select.keyBy(_._1).groupByKey().sortByKey()
    twitter_sortedGroup.saveAsTextFile(args(0))

    sc.stop()
  }
}
