package com.hackethon.spark.file.parser.core.impl

import org.apache.spark.sql.DataFrame
import com.hackethon.spark.file.parser.session.SparkSessionHandler
import com.hackethon.spark.file.parser.core.NestedFileParserTrait

class AVROFileParserImpl extends NestedFileParserTrait{
  def readFile(path:String):DataFrame={
  	val spark = SparkSessionHandler.getSparkSession()
  	return spark.read.format("com.databricks.spark.avro").load(path).repartition(4)
  }
  def readFileStream(path:String):DataFrame={
  	val spark = SparkSessionHandler.getSparkSession()
  	return spark.readStream.format("com.databricks.spark.avro").load(path)
  }
}