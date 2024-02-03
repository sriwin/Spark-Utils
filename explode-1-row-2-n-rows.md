```
%scala

import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.functions.{array, col, explode, expr, lit, monotonically_increasing_id, regexp_replace, row_number, struct}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

val fileDateString = getFormattedDate("YYYYMMDD")
val fileTimeString = getFormattedDate("HHmmssSSS")
println(s"Local Date & Time = ${fileDateString}-${fileTimeString}")

val testCaseData = List(
  Row(1, "TC-01", "A1", "input.<YYYYMMDD>.output.<xxx>.csv",
    "output.<YYYYMMDD>.output.<xxx>.csv", 11, 11),

  Row(1, "TC-02", "A2", "input.<YYYYMMDD>.output.<xxx>.csv",
    "output.<YYYYMMDD>.output.<xxx>.csv", 21, 21)
)

val testCaseSchema = StructType(
  List(
    StructField("test_id", IntegerType, false),
    StructField("test_case", StringType, false),
    StructField("vendor_code", StringType, false),
    StructField("input_file_name", StringType, false),
    StructField("output_file_name", StringType, false),
    StructField("good_records_count", IntegerType, false),
    StructField("bad_records_count", IntegerType, false)
  ))

// replace multiple placeholders in one field of dataframe
var fileDF = spark.createDataFrame(spark.sparkContext.parallelize(testCaseData), testCaseSchema)
  .withColumn("new_if", 
              regexp_replace(regexp_replace(col("input_file_name"), "<YYYYMMDD>", fileDateString),"<xxx>",fileTimeString))  
  .withColumn("new_of", 
              regexp_replace(regexp_replace(col("output_file_name"), "<YYYYMMDD>", fileDateString),"<xxx>",fileTimeString))  
  .drop("input_file_name", "output_file_name")
  .withColumnRenamed("new_if", "input_file_name")
  .withColumnRenamed("new_of", "output_file_name")

// convert 1 row to n rows based on the column of the dataframe
val df01 = fileDF.withColumn("Y", expr("explode(array_repeat(good_records_count, int(good_records_count)))"))
.withColumn("is_good_record", lit(true).cast(BooleanType))
.drop("y", "good_records_count","bad_records_count")

val df02 = fileDF.withColumn("Y", expr("explode(array_repeat(bad_records_count, int(bad_records_count)))"))
.withColumn("is_good_record", lit(false).cast(BooleanType))
.drop("y", "bad_records_count","bad_records_count")

// generate row numbers for each records in the database for joining purpose
val window = Window.orderBy(col("idx"))
var testDF = df01.unionAll(df01).withColumn("idx", monotonically_increasing_id())
testDF = testDF.withColumn("id1", row_number().over(window)).drop("idx")
//testDF.show(false)

// 
val dbQuery = "select distinct emp_id, ROW_NUMBER() OVER (ORDER BY emp_id) AS id2 from emp_db.emp"
val dbDF = spark.sql(dbQuery)

// join dataframes (testDF + dbDF using id1 and id2)
val finalDF = testDF.join(dbDF, col("id1") === col("id2"), "inner").drop("id1", "id2")
println("df01 size = " + df01.count + " && df02 size = " + df02.count + " && unionDF szie =" + testDF.count + " && finalDF = "+finalDF.count)
finalDF.show(false)

//#####################
def getFormattedDate(dateFormat: String): String = {
  val zoneId = ZoneId.of("America/Los_Angeles")
  val dateTime = LocalDateTime.now.atZone(zoneId)
  DateTimeFormatter.ofPattern(dateFormat).format(dateTime.withZoneSameInstant(zoneId))
}
```
