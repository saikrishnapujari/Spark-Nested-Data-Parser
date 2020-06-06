package com.hackethon.spark.file.parser.driver

import com.hackethon.spark.file.parser.core.NestedFileParserFactory
import com.hackethon.spark.file.parser.constants.FlattenStrategy

object NestedDataParserStreamDriver extends App {
  if(args.length < 2){
		println("Not engough Arguments!")
		System.exit(1)
	}

	val fileType = args(0)
	val filePath = args(1)
	
	val parser = NestedFileParserFactory.getParser(fileType)
	val df = parser.readFileStream(filePath)
	val dfParsed = parser.flatten(df, FlattenStrategy.SCHEMA_ITERATIVE)
	
	dfParsed.show()
	println("count :"+dfParsed.count())
}