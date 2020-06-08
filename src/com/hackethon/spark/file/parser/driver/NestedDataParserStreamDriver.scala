package com.hackethon.spark.file.parser.driver

import com.hackethon.spark.file.parser.core.NestedFileParserFactory
import com.hackethon.spark.file.parser.constants.FlattenStrategy

object NestedDataParserStreamDriver extends App {
  if(args.length < 3){
		println("Not engough Arguments!")
		System.exit(1)
	}

	val fileType = args(0)
	val filePath = args(1)
	val outputPath = args(2)
	println("fileType :"+fileType)
	println("filePath :"+filePath)
	println("outputPath :"+outputPath)
	
	try{
		val parser = NestedFileParserFactory.getParser(fileType)
		val df = parser.readFileStream(filePath)
		val dfParsed = parser.flatten(df, FlattenStrategy.SCHEMA_ITERATIVE)
	
		dfParsed.show()
		println("Final DF record count:"+dfParsed.count())
		parser.writeFile(df, outputPath)
	}catch{
		case e:Exception=> println("Exception message:"+e.getMessage)
											e.printStackTrace()
	}
}