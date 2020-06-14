package com.hackethon.spark.file.parser.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType,StructType,StructField}
import org.apache.spark.sql.functions.{col,explode,to_json}
import com.hackethon.spark.file.parser.constants.FlattenStrategy
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery


trait NestedFileParserTrait {
  
	def readFile(path:String,spark:SparkSession):DataFrame
	
	def readFileStream(path:String,spark:SparkSession,schema:StructType):DataFrame
	
	def writeFile(df:DataFrame,path:String){
		df.write.mode(SaveMode.Overwrite).csv(path)
	}
	
	def writeStream(df:DataFrame,path:String):StreamingQuery={
		df.writeStream.start(path)
	}
	
	def flatten(df:DataFrame, strategy:String): DataFrame = {
		strategy match {
			case FlattenStrategy.SCHEMA_ITERATIVE => return flattenIterative(df)
			case FlattenStrategy.SCHEMA_RECURSIVE => return flattenRecursive(df)
			case _ => println("Undefined Strategy, Default will be applied")
								return flattenIterative(df)
		}
	}
  
  protected def flattenIterative(dfGlobal: DataFrame): DataFrame = {
		var df: DataFrame = dfGlobal
		var schema: StructType = df.schema
		var flag = true //allow first loop
		 while(flag){
			flag = false //reset every loop
			schema.fields.foreach {
				elem =>
				elem.dataType match {
				case _: ArrayType => //println("flatten array")
					flag = true
					df = df.withColumn(elem.name + "_temp", explode(col(elem.name)))
									.drop(col(elem.name))
									.withColumnRenamed(elem.name + "_temp", elem.name)
				case _: StructType => //println("flatten struct")
					flag = true
					var innerSchema: StructType = null
					elem.isInstanceOf[StructField] match {
						case true =>
							val innerElem = elem.asInstanceOf[StructField]
							innerSchema = innerElem.dataType.asInstanceOf[StructType]
						case false =>
							innerSchema = elem.asInstanceOf[StructType]
					}
					innerSchema.fields.foreach {
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
          val fieldNamesWithoutArray = fieldNames.filter(_!=fieldName)
          val fieldNamesAndExplode = fieldNamesWithoutArray ++ Array(s"explode_outer($fieldName) as $fieldName")
          val explodedDf = df.selectExpr(fieldNamesAndExplode:_*)
          return flattenRecursive(explodedDf)
        case structType: StructType => //println("flatten struct")
          val innerFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ innerFieldnames
          val renamedCols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
         val explodedf = df.select(renamedCols:_*)
          return flattenRecursive(explodedf)
        case _ => //println("other type")
      }
    }
    df
  }
  
}