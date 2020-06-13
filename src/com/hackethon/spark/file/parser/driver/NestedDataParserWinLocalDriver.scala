package com.hackethon.spark.file.parser.driver

import com.hackethon.spark.file.parser.core.NestedFileParserFactory
import com.hackethon.spark.file.parser.constants.FlattenStrategy
import com.hackethon.spark.file.parser.session.SparkSessionHandler

object NestedDataParserWinLocalDriver extends App {
  
	/*
	 * For testing in windows os - with eclipse
	 * Steps::
	 *
	 * Create the following directory structure: "C:\hadoop_home\bin" (or replace "C:\hadoop_home" with whatever you like)
	 * Download the following file: http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe
	 * Put the file from step 2 into the "bin" directory from step 1.
	 * Set the "hadoop.home.dir" system property to "C:\hadoop_home" (or whatever directory you created in step 1, without the "\bin" at the end). Note: You should be declaring this property in the beginning of your Spark code
	 */
	sys.props.+=(("hadoop.home.dir", "C:\\hadoop_home"))

	if(args.length < 3){
		println("Not engough Arguments!")
		System.exit(1)
	}

	val fileType = args(0)
	val filePath = args(1)
	val outputPath = args(2)
	val flattenType = args(3)
	println("fileType :"+fileType)
	println("filePath :"+filePath)
	println("outputPath :"+outputPath)
	val spark = SparkSessionHandler.getSparkSessionLocal()
	try{
		val parser = NestedFileParserFactory.getParser(fileType)
		val df = parser.readFile(filePath,spark)
		val dfParsed = if(flattenType.equals("1")){parser.flatten(df, FlattenStrategy.SCHEMA_ITERATIVE)}else if(flattenType.equals("2")){parser.flatten(df, FlattenStrategy.SCHEMA_RECURSIVE)}else{parser.flatten(df, FlattenStrategy.SCHEMA_ITERATIVE)}
	
		dfParsed.show()
		println("Final DF record count:"+dfParsed.count())
		parser.writeFile(dfParsed, outputPath)
	}catch{
		case e:Exception=> println("Exception message:"+e.getMessage)
											e.printStackTrace()
	}finally{
		spark.stop()
	}
	
}