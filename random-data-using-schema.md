## Build Random Data using Schema

```
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDate
import scala.util.Random

val startDate = LocalDate.of(1970, 1, 1)
val endDate = LocalDate.of(2015, 1, 1)

val dynamicValues: Map[(String, DataType), Option[() => Any]] = Map(
  ("dbl", DoubleType) -> Some(() => randomDouble(1001.01, 9999.99, 1, 2)),
  ("str", StringType) -> Some(() => randomString(12)),
  ("int", IntegerType) -> Some(() => randomInt(1001, 9999)),
  ("lng", LongType) -> Some(() => randomLong(1001, 9999)),
  ("tms", TimestampType) -> Some(() => randomTimestamp(startDate, endDate)),
  ("dat", DateType) -> Some(() => randomDate(startDate, endDate)),
  ("bol", BooleanType) -> Some(() => randomBoolean)
)

val schemas = Map(
  "schema1" -> StructType(
    List(
      StructField("dbl", DoubleType, nullable = true),
      StructField("str", StringType, nullable = true),
      StructField("lng", LongType, nullable = true),
      StructField("int", IntegerType, nullable = true),
      StructField("tms", TimestampType, nullable = true),
      StructField("dat", DateType, nullable = true),
      StructField("bol", BooleanType, nullable = true)
    )),
  "schema2" -> StructType(
    List(
       StructField("dbl", DoubleType, nullable = true),
      StructField("str", StringType, nullable = true),
      StructField("lng", LongType, nullable = true),
      StructField("int", IntegerType, nullable = true),
      StructField("tms", TimestampType, nullable = true),
      StructField("dat", DateType, nullable = true),
      StructField("bol", BooleanType, nullable = true)
    )
  )
)
val numRecords = 1000

schemas.foreach {
  case (name, schema) =>
    val rows = (0 until numRecords).map { _ =>
      Row.fromSeq(schema.fields.map(field => {
        dynamicValues.get((field.name, field.dataType)).flatMap(_.map(f => f())).orNull
      }))
    }.toList
    
    val sparkRows = spark.sparkContext.parallelize(rows)
    val df = spark.createDataFrame(sparkRows, schema)
    df.show(false)
}

def randomInt(startNbr: Int, endNbr: Int): Integer = {
  val r = new scala.util.Random
  startNbr + r.nextInt((endNbr - startNbr) + 1)
}

def randomLong(startNbr: Int, endNbr: Int): Long = {
  randomInt(startNbr: Int, endNbr: Int).asInstanceOf[Number].longValue
}
  
def randomDouble(startNbr: Double, endNbr: Double, by: Double, precision: Int): Double = {
  BigDecimal(((startNbr / endNbr) * by)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
}

def randomString(length: Int) = scala.util.Random.alphanumeric.take(length).mkString

def randomDate(startDate: LocalDate, endDate: LocalDate): java.sql.Date = {
  java.sql.Date.valueOf(startDate.plusDays(Random.nextInt((endDate.toEpochDay - startDate.toEpochDay) toInt)))
}

def randomTimestamp(startDate: LocalDate, endDate: LocalDate): Timestamp = {
  val date1 = Date.valueOf(startDate).getTime
  val date2 = Date.valueOf(endDate).getTime
  val diff = date2 - date1
  new Timestamp(date1 + (Random.nextFloat() * diff).toLong)
}

def randomBoolean = (scala.util.Random.nextInt(2) == 0)
```
