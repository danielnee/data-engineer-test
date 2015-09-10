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
 */
object SalesTransform {

  def transformData(customer : RDD[Customer], header : RDD[Transaction], promotions : RDD[Promotion], line : RDD[Line]) : RDD[Sale] = {
    // You should remove these 3 lines and implement the function
    // They are just they to make the code compile
    val conf = new SparkConf().setAppName("Data Engineer Test") // STUB
    val sc = new SparkContext(conf) // STUB
    sc.parallelize(Array[Sale]()) // STUB
  }

  def main(args : Array[String]) : Unit = {

    val conf = new SparkConf().setAppName("Data Engineer Test")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    sc.parallelize(Array[Sale]())

    // All data files are TSV

    // Customer data is located in HDFS under /tesco/customer
    // The DOB of customers has been sourced from multiple systems and is in different formats
    // You should clean this

    // Header data is located in HDFS under /tesco/header

    // Promotion data is located in HDFS under /tesco/promotion

    // Line data is located in HDFS under /tesco/line

    // 1. Implement the main function to read all data files in and transform them in RDD's of the case classes
    // 2. Clean up the DOB of customers.
    // 3. Implement transformData to transform the data into Sale case class
    // 4. Write the Sale RDD to a Parquet file


    // Unit test as much as possible. You won't have access to a live cluster for this test, so think how you can test your code as much as possible
    // Structure your code in a way you see fit (add new classes, split code into multiple files, change package structure etc.)
    // Remember we may want to re-use logic e.g. we may need to transform the customer data in the same way in several locations. How would you structure your code to achieve this?
    // Add any additional libraries you want to use
    // If you make any assumptions, please comment about them in your code
    // Example TSV files are included under resources. You can assume the columns and ordering of the case classes correspond exactly to the TSV's


  }

}
