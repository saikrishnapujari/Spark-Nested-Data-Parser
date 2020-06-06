package com.hackethon.spark.file.parser.session

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkSessionHandler {
  def getSparkSession():SparkSession={
		return SparkSession.builder().master("local").appName("Sample").getOrCreate()
  }
  
  def getSparkStreamSession():SparkSession={
		return SparkSession.builder().master("local").appName("Sample").getOrCreate()
  }
}