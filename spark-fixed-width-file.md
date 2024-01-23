# Spark - Generate Fixed Width File

```
%scala

import org.apache.spark.sql.{Encoder, Row, SparkSession}
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions.{col, rpad, trim}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import spark.implicits._

val metadata = Seq(
  ("first_name", "6"),
  ("last_name ", "7"),
  ("email", "8"),
  ("phone_number", "9"),
  ("salary", "10")
).toDF("field_name", "field_length")

val colNames = metadata.select("field_name").rdd.map(x => x.getString(0).trim()).collect()
val colSize = metadata.select("field_length").rdd.map(x => x.getString(0).trim()).collect().map(_.toInt).toList
val structFields = colNames.map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(structFields)

val dummyData = List(
  Row("f01", "l01", "e01", "P1", "S1"),
  Row("f02", "l02", "e02", "P2", "S2"),
  Row("f03", "l03", "e03", "P3", "S3"),
)

val df01 = spark.createDataFrame(spark.sparkContext.parallelize(dummyData), schema)
val df02 = df01.columns.zip(colSize).foldLeft(df01) {
  (newdf, colname) => newdf.withColumn(colname._1, rpad(trim(col(colname._1).cast("string")), colname._2, "*"))
}
df02.show(false)
```
