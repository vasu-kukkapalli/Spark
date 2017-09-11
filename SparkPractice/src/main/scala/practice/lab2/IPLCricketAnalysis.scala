package practice.lab2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

object IPLCricketAnalysis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("IPL Cricket").setMaster("local[*]").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val input = "/home/vara/Vasu/Spark/Practice/data/matches.csv"
    val outputDir = "/home/vara/Vasu/spark-srini/DEV3600_LAB_DATA/data/output/"
    
    val data = sc.textFile(input)
    
    
    
//    println("Data count %s".format(data.count()))

    val filtering_bad_records = data.map(line => line.split(",")).filter(x => x.length < 19)
    
//    println("After filter Data count %s".format(filtering_bad_records.count()))

    /*So we will take out the columns toss_decision, won_by_runs, won_by_wickets, venue. 
    From this we will filter out the columns which are having won_by_runs value as 0 so that we can get the teams which won by batting first*/    
    val extracting_columns = filtering_bad_records.map(x => (x(7), x(11), x(12), x(14))) //.collect.foreach(println)
    
    //  1.Which stadium is best suitable for first batting
    val bat_first_won = extracting_columns.filter(x => x._2 != "0").map(x => (x._4, 1)).reduceByKey(_ + _)//.map(item => item.swap).sortByKey(false).collect.foreach(println)
    val total_matches_per_venue = filtering_bad_records.map(x=>(x(14),1)).reduceByKey(_+_)//.map(item => item.swap).sortByKey(false).collect.foreach(println)
    val join = bat_first_won.join(total_matches_per_venue).map(x=>(x._1,(x._2._1*100/x._2._2))).map(item => item.swap).sortByKey(false).collect.foreach(println)
    
    //  2.Which stadium is best suitable for first bowling
    val bowl_first_won = extracting_columns.filter(x=>x._3!="0").map(x=>(x._4,1)).reduceByKey(_+_)//.map(item => item.swap).sortByKey(false).collect.foreach(println)
    val join1 = bowl_first_won.join(total_matches_per_venue).map(x=>(x._1,(x._2._1*100/x._2._2))).map(item => item.swap).sortByKey(false).collect.foreach(println)

  }

}