package com.hackethon.spark.file.parser.core.impl

import com.hackethon.spark.file.parser.core.NestedFileParserTrait
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class TextFileParserImpl extends NestedFileParserTrait{
  def readFile(path:String,spark:SparkSession):DataFrame={
  	return spark.read.text(path).repartition(4)
  }
  def readFileStream(path:String,spark:SparkSession):DataFrame={
  	return spark.readStream.text(path)
  }
}