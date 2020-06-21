package com.hackethon.spark.file.parser.session

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
/**
 * @author Sai Krishna P
 */
object SparkSessionHandler {
  def getSparkSession():SparkSession={
		return SparkSession.builder().appName("SparkNestedDataParser").getOrCreate()
  }
  
  def getSparkStreamSession():SparkSession={
		return SparkSession.builder().appName("SparkNestedDataParser").getOrCreate()
  }
  
  def getSparkSessionLocal():SparkSession={
		return SparkSession.builder().master("local").appName("SparkNestedDataParser").getOrCreate()
  }

}