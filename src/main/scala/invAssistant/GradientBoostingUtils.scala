package invAssistant

import org.apache.spark.rdd._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.tree.configuration.BoostingStrategy

import scala.collection.mutable.ArrayBuffer
import scala.util.{Success, Try}

object GradientBoostingUtils { 
  
  // this is a driver function
  def run_training(dataset:DataFrame):GradientBoostedTreesModel = {
    // 1. Add label 1 or 0
    val data_labeled = add_label_to_dataset(dataset)
    data_labeled.take(10).foreach(println)
    println("lable complete")
    
    // 2. Split the dataset into train-set and valid-set
    val (train_set, valid_set) = train_valid_split(data_labeled)
    //train_set.take(10).foreach(println)
    //valid_set.take(10).foreach(println)
    println("split complete")
  
    // 3. Run machine learning algorithm
    val model = run_machine_learning(train_set, valid_set)
    println("training complete")
    
    return model
  }

  def add_label_to_dataset(dataset:DataFrame):RDD[LabeledPoint] ={
    /* input: dataset after cleaning
     * output: dataset with corresponding label
     */
    val messagesRDD = dataset.rdd
    
    //We use scala's Try to filter out data that couldn't be parsed
    val goodBadRecords = messagesRDD.map(
          row =>{
              Try{
                  val msg = row(0).toString.toLowerCase()
                  var isHappy:Int = 0
                  if(msg.contains("sad")){
                    isHappy = 0
                  }else if(msg.contains("happy")){
                    isHappy = 1 
              }
              var msgSanitized = msg.replaceAll("happy", "")
              msgSanitized = msgSanitized.replaceAll("sad","")
              //Return a tuple   
              (isHappy, msgSanitized.split(" ").toSeq)
              }
          })
    
    //We use this syntax to filter out exceptions
    val exceptions = goodBadRecords.filter(_.isFailure) 
    println("total records with exceptions: " + exceptions.count())
    exceptions.take(10).foreach(x => println(x.failed))
    
    var labeledTweets = goodBadRecords.filter((_.isSuccess)).map(_.get) 
    println("total records with successes: " + labeledTweets.count())
    val hashingTF = new HashingTF(2000)

    //Map the input strings to a tuple of labeled point + input text
    val input_labeled = labeledTweets.map(
      t => (t._1, hashingTF.transform(t._2))).map(
        x => new LabeledPoint((x._1).toDouble, x._2))
      
    return input_labeled
  }


  def train_valid_split(input_labeled:RDD[LabeledPoint]):(RDD[LabeledPoint], RDD[LabeledPoint]) = {
    /* input: dataset with label (output from add_label_to_dataset)
     * output: two dataset: training set and validation set
     */
    val splits = input_labeled.randomSplit(Array(0.7, 0.3))
    val (trainingData, validationData) = (splits(0), splits(1))
    
    return (trainingData, validationData)
  }

  def run_machine_learning(trainingData:RDD[LabeledPoint], validationData:RDD[LabeledPoint]):GradientBoostedTreesModel = {
    /*---- Build the Model ----*/
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.setNumIterations(20) //number of passes over our training data
    boostingStrategy.treeStrategy.setNumClasses(2) //We have two output classes: happy and sad
    boostingStrategy.treeStrategy.setMaxDepth(5) 
    /* Depth of each tree. Higher numbers mean more parameters, which can cause overfitting.
     * Lower numbers create a simpler model, which can be more accurate.
     * In practice you have to tweak this number to find the best value.
     */
    
    println("Now, let's start training...")
    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)
    
    println("start gradient.")
    
    // Evaluate model on test instances and compute test error
    var labelAndPredsTrain = trainingData.map { point =>
      val prediction = model.predict(point.features)
      Tuple2(point.label, prediction)
    }
    
    var labelAndPredsValid = validationData.map { point =>
      val prediction = model.predict(point.features)
      Tuple2(point.label, prediction)
    } 
    
    println("prepare complete")
    //Since Spark has done the heavy lifting already, lets pull the results back to the driver machine.
    //Calling collect() will bring the results to a single machine (the driver) and will convert it to a Scala array.

    
    //Start with the Training Set
    val results = labelAndPredsTrain.collect()
    
    var happyTotal = 0
    var unhappyTotal = 0
    var happyCorrect = 0
    var unhappyCorrect = 0
    
    results.foreach(
      r => {
        if (r._1 == 1) {
          happyTotal += 1
        } else if (r._1 == 0) {
          unhappyTotal += 1
        }
        if (r._1 == 1 && r._2 ==1) {
          happyCorrect += 1
        } else if (r._1 == 0 && r._2 == 0) {
          unhappyCorrect += 1
        }
      })
    
    println("unhappy messages in Training Set: " + unhappyTotal + " happy messages: " + happyTotal)
    println("happy % correct: " + happyCorrect.toDouble/happyTotal)
    println("unhappy % correct: " + unhappyCorrect.toDouble/unhappyTotal)
    
    val testErr = labelAndPredsTrain.filter(r => r._1 != r._2).count.toDouble / trainingData.count()
    println("Test Error Training Set: " + testErr)
    return model
  }

}
