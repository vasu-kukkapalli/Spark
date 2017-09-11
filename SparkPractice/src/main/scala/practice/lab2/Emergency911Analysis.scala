package practice.lab2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

object Emergency911Analysis {
  
   case class csv911CC(lat:String ,lng:String ,desc:String ,zip:String ,title:String ,timeStamp:String ,twp:String ,addr:String ,e:String)
   case class zipCodeCC(zip:String ,city:String ,state:String ,lat:String ,long:String ,timezone:String ,dst:String)
   
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Emergency 911 Analysis").setMaster("local[*]").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    val outputPath = "/home/vara/Vasu/Spark/Practice/data/output"
    val csv911FilePath = "/home/vara/Vasu/Spark/Practice/data/911.csv"
    
    val csv911WHRDD = sc.textFile(csv911FilePath)
    
    val csvheader = csv911WHRDD.first()
    val csv911RDD = csv911WHRDD.filter(_!= csvheader)
    //lat,lng,desc,zip,title,timeStamp,twp,addr,e
    val csv911DF = csv911RDD.map(_.split(",")).filter(_.length>=9).map(x=>csv911CC(x(0),x(1),x(2),x(3),x(4).substring(0,x(4).indexOf(":")),x(5),x(6),x(7),x(8))).toDF
    csv911DF.registerTempTable("csv911Table")
    //csv911DF.show
    
    
    val zipCodefilepath = "/home/vara/Vasu/Spark/Practice/data/zipcode.csv"
    val zipCodeWHRDD = sc.textFile(zipCodefilepath)
    val zipCodeHeader = zipCodeWHRDD.first()
    val zipCodeRDD = zipCodeWHRDD.filter(_!= zipCodeHeader)
    //zip	city	state	latitude	longitude	timezone	dst
    val zipCodeDF = zipCodeRDD.map(_.split(",")).filter(_.length >= 7)
        .map(x=>zipCodeCC(x(0).replace("\"",""),x(1).replace("\"", ""),x(2).replace("\"", ""),x(3),x(4),x(5),x(6))).toDF
    zipCodeDF.registerTempTable("zipCodeTable")
    zipCodeDF.show
    val join1 = sqlContext.sql("select e.title, z.city,z.state from csv911Table e join zipCodeTable z on e.zip = z.zip")
    //What kind of problems are prevalent, and in which state?
    val sel1 = join1.map(x=>(x(0)+"-->"+x(2).toString))
    val result_state = sel1.map((_,1)).reduceByKey(_+_).map(_.swap).sortByKey(false).repartition(1).saveAsTextFile(outputPath)
    
    //What kind of problems are prevalent, and in which city?
    val sel2 = join1.map(x=>(x(0)+"-->"+x(1).toString))
    val result_city = sel2.map((_,1)).reduceByKey(_+_).map(_.swap).sortByKey(false).repartition(1).saveAsTextFile(outputPath + "/output")

  }
}  