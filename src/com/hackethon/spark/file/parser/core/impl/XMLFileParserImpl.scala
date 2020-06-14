package com.hackethon.spark.file.parser.core.impl

import org.apache.spark.sql.DataFrame
import com.hackethon.spark.file.parser.core.NestedFileParserTrait
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class XMLFileParserImpl extends NestedFileParserTrait{
  def readFile(path:String,spark:SparkSession):DataFrame={
  	return spark.read.format("com.databricks.spark.xml").option("rowTag", "root").load(path).repartition(4)
  }
  def readFileStream(path:String,spark:SparkSession,schema:StructType):DataFrame={
  	return spark.readStream.schema(schema).format("com.databricks.spark.xml").load(path)
  }
}