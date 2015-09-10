package com.tesco

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

/**
 * A customer
 */
case class Customer (customerId : String, // Unique customer identifier
                     name : String, // Customer Name
                     dob : String) // Customer date of birth

/**
 * A transaction
 */
case class Transaction (transactionId : String, // Unique transaction identifier
                        customerId : String, // Unique customer identifier
                        transactionValueGbpInPence : Int, // Value of transaction in GBP, pence, e.g. 300 is £3.00
                        items : Int) // Number of items bought in transaction

/**
 * Promotions a customer triggered on transaction
 */
case class Promotion (transactionId : String, // Unique transaction identifier
                      promotionId : String, // Unique promotion identifier
                      promotionValueGbpInPence : Int) // Value of the promotion triggered

/**
 * Products bought as part of a transaction
 */
case class Line (transactionId : String, // Unique transaction identifier
                 productId : String, // Unique product identifier
                 quantity : Int, // Number of product bought
                 priceGbpInPence : Int, // Price per product in GBP, pence, e.g. 300 is £3.00
                 amountGbpInPence : Int) // Total amount paid for product e.g. quantity * priceGbpInPence

/**
 * Transformed transactional data that will be using for analytics purposes
 */
case class Sale (customerId : String, // Unique customer identifier
                 yearOfBirth : String, // Customer year of birth e.g 1980
                 transactionId : String, // Unique transaction identifier
                 transactionValueGbp : Double, // Value of transaction in GBP e.g. £3.00
                 items : Int, // Number of items in purchased
                 numberPromotions : Int, // Total number of promotions triggered
                 promotionTotalValueInGbp : Double, // Total value of all the promotions
                 averageItemValueInGbp : Double) // Average value of the items purchased

/**
 * Data Engineer Test
 *
 * This job will read the input files and transform the data to the desired output format (Sale case class) and write the results out *
 */
object SalesTransform {

  def transformData(customer : RDD[Customer], header : RDD[Transaction], promotions : RDD[Promotion], line : RDD[Line]) : RDD[Sale] = {
    // You should remove these 3 lines and implement the function
    // They are just there to make the code compile
    val conf = new SparkConf().setAppName("Data Engineer Test") // STUB
    val sc = new SparkContext(conf) // STUB
    sc.parallelize(Array[Sale]()) // STUB
  }

  def main(args : Array[String]) : Unit = {

    val conf = new SparkConf().setAppName("Data Engineer Test")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // All data files are TSV

    // Customer data is located in HDFS under /tesco/customer
    // The DOB of customers has been sourced from multiple systems and is in different formats
    // You should clean this

    // Header data is located in HDFS under /tesco/header

    // Promotion data is located in HDFS under /tesco/promotion

    // Line data is located in HDFS under /tesco/line

  }

}
