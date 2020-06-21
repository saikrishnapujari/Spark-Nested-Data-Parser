# Spark-Nested-Data-Parser
 Nested Data (JSON/AVRO/XML) Parsing and Flattening in Spark

Implementation steps:
Load JSON/XML to a spark data frame.
Loop until the nested element flag is set to false.
Loop through the schema fields - set the flag to true when we find ArrayType and StructType.
For ArrayType - Explode and StructType - separate the inner fields.
It comes out once all the levels are flattened out.

Flatten Strategy:
Schema fields iteration using 1.Iterative 2.Recursive
