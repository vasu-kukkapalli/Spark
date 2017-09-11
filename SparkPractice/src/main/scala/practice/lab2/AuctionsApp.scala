package practice.lab2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.lang.Math

object AuctionsApp {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Lab2").setMaster("local[*]").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val outputDir = "/home/vara/Vasu/spark-srini/DEV3600_LAB_DATA/data/output/"

    val auctionRDD = sc.textFile("/home/vara/Vasu/spark-srini/DEV3600_LAB_DATA/data/auctiondata.csv").map(_.split(",")).cache()

//    val auCount = auRDD.count()
    val auctionid = 0
    val bid = 1
    val bidtime = 2
    val bidder = 3
    val bidderrate = 4
    val openbid = 5
    val price = 6
    val itemtype = 7
    val daystolive = 8
    val totalbids=auctionRDD.count()
//    val totlItems = auctionRDD.map(x => (x(7), 1)).reduceByKey(_ + _).saveAsTextFile(outputDir)
    
    //total number of items (auctions)
    val totalitems=auctionRDD.map(line=>line(auctionid)).distinct().count()
    //RDD containing ordered pairs of auctionid,number
    val bids_auctionRDD=auctionRDD.map(line=>(line(auctionid),1)).reduceByKey((x,y)=> x+y)
    //max, min and avg number of bids
    val maxbids=bids_auctionRDD.map(x=>x._2).reduce((x,y)=>Math.max(x,y))
    val minbids=bids_auctionRDD.map(x=>x._2).reduce((x,y)=>Math.min(x,y))
    val avgbids=totalbids/totalitems
    println("total bids across all auctions: %s" .format(totalbids))
    println("total number of distinct items: %s" .format(totalitems))
    println("Max bids across all auctions: %s ".format(maxbids))
    println("Min bids across all auctions: %s ".format(minbids))
    println("Avg bids across all auctions: %s ".format(avgbids))
    
  }

}