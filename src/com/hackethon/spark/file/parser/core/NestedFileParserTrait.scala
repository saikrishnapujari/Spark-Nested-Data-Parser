package com.hackethon.spark.file.parser.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType,StructType,StructField}
import org.apache.spark.sql.functions.{col,explode,explode_outer,to_json}
import com.hackethon.spark.file.parser.constants.FlattenStrategy
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * @author Sai Krishna P
 */
trait NestedFileParserTrait {
  
	def readFile(path:String,spark:SparkSession):DataFrame
	
	def readFileStream(path:String,spark:SparkSession,schema:StructType):DataFrame
	
	def writeFile(df:DataFrame,path:String){
		df.write.mode(SaveMode.Overwrite).csv(path)
	}
	
	def writeStream(df:DataFrame,path:String):StreamingQuery={
		df.writeStream.start(path)
	}
	
	/**
	 * @param df
	 * @param strategy
	 * @return
	 */
	def flatten(df:DataFrame, strategy:String): DataFrame = {
		strategy match {
			case FlattenStrategy.SCHEMA_ITERATIVE => return flattenIterativeV2(df)
			case FlattenStrategy.SCHEMA_RECURSIVE => return flattenRecursive(df)
			case _ => println("Undefined Strategy, Default will be applied")
								return flattenIterative(df)
		}
	}
  
  /**
   * Iterative Schema flattening 
 * @param dfGlobal
 * @return
 */
protected def flattenIterative(dfGlobal: DataFrame): DataFrame = {
		var df: DataFrame = dfGlobal
		var schema: StructType = df.schema
		var flag = true //allow first loop
		 while(flag){
			flag = false //reset every loop
			schema.fields.foreach {
				elem =>
				elem.dataType match {
				case arrayType: ArrayType => //println("flatten array")
					flag = true
					df = df.withColumn(elem.name + "_temp", explode_outer(col(elem.name)))
								 .drop(col(elem.name))
								 .withColumnRenamed(elem.name + "_temp", elem.name)
				case structType: StructType => //println("flatten struct")
					flag = true
					structType.fields.foreach {
						inElem =>
						df = df.withColumn(elem.name + "_" + inElem.name, col(elem.name + "." + inElem.name))
					}
					df = df.drop(col(elem.name))
				case _ => //println("other type")
				}
			}
			schema = df.schema
		}
		return df
	}

/**
   * Iterative Schema flattening - Version2
 * @param dfGlobal
 * @return
 */
protected def flattenIterativeV2(dfGlobal: DataFrame): DataFrame = {
		var df: DataFrame = dfGlobal
		var flag = true //allow first loop
		 while(flag){
			flag = false //reset every loop
			df.schema.fields.foreach {
				elem =>
				var fieldNames = df.schema.fields.map(x => x.name)
				elem.dataType match {
				case arrayType: ArrayType => //println("flatten array")
					flag = true
					fieldNames = fieldNames.filter(_!=elem.name) ++ Array("explode_outer(".concat(elem.name).concat(") as ").concat(elem.name))
					df=df.selectExpr(fieldNames:_*)
				case structType: StructType => //println("flatten struct")
					flag = true
					fieldNames = fieldNames.filter(_!=elem.name) ++ 
          										structType.fieldNames.map(childname => elem.name.concat(".").concat(childname)
          																																		.concat(" as ")
          																																		.concat(elem.name).concat("_").concat(childname))
					df=df.selectExpr(fieldNames:_*)
				case _ => //println("other type")
				}
				
			}
		}
		return df
	}
  
  /**
   * Recursive Schema flattening
 * @param df
 * @return
 */
protected def flattenRecursive(df: DataFrame): DataFrame = {

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length
    
    for(i <- 0 to fields.length-1){
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType => //println("flatten array")
          val newfieldNames = fieldNames.filter(_!=fieldName) ++ Array("explode_outer(".concat(fieldName).concat(") as ").concat(fieldName))
          val explodedDf = df.selectExpr(newfieldNames:_*)
          return flattenRecursive(explodedDf)
        case structType: StructType => //println("flatten struct")
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ 
          										structType.fieldNames.map(childname => fieldName.concat(".").concat(childname)
          																																		.concat(" as ")
          																																		.concat(fieldName).concat("_").concat(childname))
         	val explodedf = df.selectExpr(newfieldNames:_*)
          return flattenRecursive(explodedf)
        case _ => //println("other type")
      }
    }
    df
  }
  
}