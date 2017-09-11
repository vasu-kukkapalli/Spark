package practice.lab2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

object HospitalChargeAnalysis {
  
   /*case class inpatientChargesCC(DRGDefinition:String ,ProviderId:Integer ,ProviderName:String ,ProviderStreetAddress:String ,ProviderCity:String 
       ,ProviderState:String ,ProviderZipCode:Integer ,HospitalReferralRegionDescription:String ,TotalDischarges:Integer ,AverageCoveredCharges:Double 
       ,AverageTotalPayments:Double ,AverageMedicarePayments:Double)*/
  case class inpatientChargesCC(DRGDefinition:String ,ProviderId:String ,ProviderName:String ,ProviderStreetAddress:String ,ProviderCity:String 
       ,ProviderState:String ,ProviderZipCode:String ,HospitalReferralRegionDescription:String ,TotalDischarges:String ,AverageCoveredCharges:Double 
       ,AverageTotalPayments:String ,AverageMedicarePayments:String)
   
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Hospital Charge Analysis").setMaster("local[*]").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val inpatientChargesFilePath = "/home/vara/Vasu/Spark/Practice/data/inpatientCharges.csv"
    val outputPath = "/home/vara/Vasu/Spark/Practice/data/output"
    
    val inpatientChargesDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(inpatientChargesFilePath)
    inpatientChargesDF.registerTempTable("inpatientChargesTable")
    inpatientChargesDF.groupBy("ProviderState").avg("AverageCoveredCharges")
  }
}  