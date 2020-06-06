package com.hackethon.spark.file.parser.driver

import com.hackethon.spark.file.parser.core.NestedFileParserFactory
import com.hackethon.spark.file.parser.constants.FlattenStrategy

object NestedDataParserBatchDriver extends App{
	
	if(args.length < 2){
		println("Not engough Arguments!")
		System.exit(1)
	}

	val fileType = args(0)
	val filePath = args(1)
	
	val parser = NestedFileParserFactory.getParser(fileType)
	val df = parser.readFile(filePath)
	val dfParsed = parser.flatten(df, FlattenStrategy.SCHEMA_ITERATIVE)
	
	dfParsed.show()  
}