package com.hackethon.spark.file.parser.core

import com.hackethon.spark.file.parser.constants.Constants
import com.hackethon.spark.file.parser.core.impl.JSONFileParserImpl
import com.hackethon.spark.file.parser.core.impl.TextFileParserImpl
import com.hackethon.spark.file.parser.core.impl.AVROFileParserImpl
import com.hackethon.spark.file.parser.core.impl.XMLFileParserImpl
/**
 * @author Sai Krishna P
 */
object NestedFileParserFactory {
  def getParser(fileType:String):NestedFileParserTrait={
  	fileType match {
  		case Constants.JSON => return new JSONFileParserImpl
  		case Constants.AVRO => return new AVROFileParserImpl
  		case Constants.XML => return new XMLFileParserImpl
  		case _ => println("Parser not available for file type :"+fileType+" Going with Default parser: TextFileParser")
  							return new TextFileParserImpl
  	}
  }
}