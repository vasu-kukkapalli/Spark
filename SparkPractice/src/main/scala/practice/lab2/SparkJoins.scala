package practice.lab2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

object SparkJoins {
  
  case class custclass(cust_id: Int, store_id:Int, sale:Int)
  
  case class storeclass(store_id:Int, rating:Float)

  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Spark Joins").setMaster("local[*]").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    val cust = "/home/vara/Vasu/Spark/Practice/data/joins_cust.csv"
    val store = "/home/vara/Vasu/Spark/Practice/data/joins_store.csv"
    

    val custRDD = sc.textFile(cust).map(_.split(","))
    val storeRDD = sc.textFile(store).map(_.split(","))
    
    val cust_record = custRDD.map(x => (x(1).toInt, custclass(x(0).toInt, x(1).toInt, x(2).toInt)))
    val store_record = storeRDD.map(x => (x(0).toInt, storeclass(x(0).toInt, x(1).toFloat)))
    
    val joined_rdd = cust_record.join(store_record).foreach(println)
    
    val leftjoin_rdd = cust_record.leftOuterJoin(store_record).foreach(println)
    
    val custDF = custRDD.map(x=>custclass(x(0).toInt, x(1).toInt, x(2).toInt)).toDF()//.show
    custDF.registerTempTable("custtable")
    val storeDF = storeRDD.map(x=>storeclass(x(0).toInt, x(1).toFloat)).toDF()//.show
    storeDF.registerTempTable("storetable")
    //val naiveinnerjoin = custDF.join(storeDF, custDF("store_id") === storeDF("store_id"), "inner").show
    //naiveinnerjoin.select("store_id").show() //Exception in thread "main" org.apache.spark.sql.AnalysisException: Reference 'store_id' is ambiguous, could be: store_id#1, store_id#3.;
    
    val joinedDF = custDF.as('a).join(storeDF.as('b), $"a.store_id" === $"b.store_id")//.show
    
    joinedDF.select($"a.store_id").show
    
    val myDF = sqlContext.sql("select * from custtable c join storetable s on c.store_id = s.store_id").show
    
  }

}