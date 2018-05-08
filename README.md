### Investment Assistant Based on Social Sentiment Analytics

May 2018

---
### I. Application Intro
This is an application that helps investors make timely and effective investment decisions based on news feed from social media. We help user to make the best decision by:

1. Extract stocks or industries that are spotlighted recently.
2. Design keywords list including:
	- Ticker name
	- Industry and sector.
	- Company name
3. Read and Extract tweets using Spark Sql from our database, which has alreay been stored in HDFS.
4. Draw relation curves between timestamp and:
	a. Social sentiment about certain fields or industries.
	b. Social attention about certain field or industries.
	c. Stock prices and their returns extracted from Yahoo Finance.
5. After conclude result shown above, the applicaiton will train a Machine Learning model (by Gradient Boosting algorithm) using above social sentiment and attention information as input, stock price up-and-down as labels. It gives a specific advice/straegy about whether user should invest or how to trade in this field.

### II. Accessing Datasets 
1. Tweets Coolection gathered from Twitter since 2010.1.1 to 2018.3.31
	- HDFS Location: `hdfs:///user/xl2053/BDAD_project/twitter/`
2. Reuters News Title Collection from 2010.1.1 to 2018.3.31
	- HDFS Location: `hdfs:///user/xl2053/BDAD_project/reuters/`
	- Source: [Reuters archive] (https://www.reuters.com/resources/archive/us/)
3. Stock Historic Prices from Yahoo Finance since 2010.1.1 to 2019.3.31
	- HDFS Location: `hdfs:///user/xl2053/BDAD_project/stock_dataset/`
	
	- Or download on your own using a wrapper over Yahoo! Finance API: [yahoo_historical](https://github.com/AndrewRPorter/yahoo-historical)


### III. Running Instruction

#### Using Stanford CoreNLP
We use the language model from Stanford CoreNLP (3.6.0) in our project. 

To use Stanford CoreNLP, please download the zip file from the official website or Maven Central, and make sure that all .jar files are included in the lib directory of the project. 

An option will be added to the spark-submit command, and is described later in "Running the application".

#### Using Maven
We compile and run our application on NYU HPC Dumbo cluster.

To compile our project, please enter the project directory and make sure the pom.xml file exists. Then run:
```
$ /opt/maven/bin/mvn clean package
```
#### Running the application
```
spark-submit 
	--jars $(echo lib/*.jar | tr ' ' ',') 
	--class invAssistant.InvestmentAssistant 
	--master yarn target/xialiang-bdad-project-1.0-jar-with-dependencies.jar 
	<path to input data>
	<path to save output>
	<ticker_name>
```

Here is a example that we run the application on user `xl2053` account, and we want to generate insights on Amazon stock (Ticker: AMZN):

```
spark-submit 
	--jars $(echo lib/*.jar | tr ' ' ',') 
	--class invAssistant.InvestmentAssistant 
	--master yarn target/xialiang-bdad-project-1.0-jar-with-dependencies.jar 
	/user/xl2053/BDAD_project/ 
	/user/xl2053/BDAD_project/AllTestOutput/ 
	AMZN
```
