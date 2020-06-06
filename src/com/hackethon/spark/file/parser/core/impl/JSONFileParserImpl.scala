package com.hackethon.spark.file.parser.core.impl

import com.hackethon.spark.file.parser.core.NestedFileParserTrait
import org.apache.spark.sql.DataFrame
import com.hackethon.spark.file.parser.session.SparkSessionHandler

class JSONFileParserImpl extends NestedFileParserTrait{
  def readFile(path:String):DataFrame={
  	val spark = SparkSessionHandler.getSparkSession()
  	return spark.read.json(path).repartition(4)
  }
  def readFileStream(path:String):DataFrame={
  	val spark = SparkSessionHandler.getSparkSession()
  	return spark.readStream.json(path)
  }
}