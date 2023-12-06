import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType,DoubleType}
import vegas._
import vegas.sparkExt.VegasSpark

object SparkDatasetAnalysis {
  def main(args: Array[String]): Unit = {

    // Создаем SparkContext
    val NODES = 4
    val spark = SparkSession.builder.appName("Analyze Dataset").master(s"local[$NODES]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    val schema = new StructType()
      .add("name", StringType, true)
      .add("department", StringType, true)
      .add("job", StringType, true)
      .add("date_started", StringType, true)
      .add("current_position", StringType, true)
      .add("date_term", StringType, true)
      .add("sex", StringType, true)
      .add("status", StringType, true)
      .add("race", StringType, true)
      .add("regular_pay", DoubleType,true)
      .add("premium_pay", DoubleType,true)
      .add("other_pay", DoubleType,true)
      .add("total_pay", DoubleType,true)

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    val df = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load("C:\\bigdata\\salary2018.csv")

    df.printSchema()
    df.show(10)
    println("Number of null/Nan values:")
    println("name", df.filter(df("name").isNull || df("name") === "" || df("name").isNaN).count())
    println("department", df.filter(df("department").isNull || df("department") === "" || df("department").isNaN).count())
    println("job", df.filter(df("job").isNull || df("job") === "" || df("job").isNaN).count())
    println("date_started", df.filter(df("date_started").isNull || df("date_started") === "" || df("date_started").isNaN).count())
    println("current_position", df.filter(df("current_position").isNull || df("current_position") === "" || df("current_position").isNaN).count())
    println("date_term", df.filter(df("date_term").isNull || df("date_term") === "" || df("date_term").isNaN).count())
    println("sex", df.filter(df("sex").isNull || df("sex") === "" || df("sex").isNaN).count())
    println("status", df.filter(df("status").isNull || df("status") === "" || df("status").isNaN).count())
    println("race", df.filter(df("race").isNull || df("race") === "" || df("race").isNaN).count())
    println("regular_pay", df.filter(df("regular_pay").isNull || df("regular_pay") === "" || df("regular_pay").isNaN).count())
    println("premium_pay", df.filter(df("premium_pay").isNull || df("premium_pay") === "" || df("premium_pay").isNaN).count())
    println("other_pay", df.filter(df("other_pay").isNull || df("other_pay") === "" || df("other_pay").isNaN).count())
    println("total_pay", df.filter(df("total_pay").isNull || df("total_pay") === "" || df("total_pay").isNaN).count())

    Vegas("Avg_Sex_Pay").
      withDataFrame(df).
      encodeX("sex", Ordinal).
      encodeY(field = "total_pay", Quantitative, aggregate =
        AggOps.Average).
      mark(Bar).
      show

    Vegas("Avg_Race_Pay").
      withDataFrame(df).
      encodeY("race", Ordinal).
      encodeX(field = "total_pay", Quantitative, aggregate =
        AggOps.Average).
      mark(Bar).
      show

    Vegas("Avg_Department_Pay").
      withDataFrame(df).
      encodeY("department", Ordinal).
      encodeX(field = "total_pay", Quantitative, aggregate =
        AggOps.Average).
      mark(Bar).
      show

    Vegas("Status").
      withDataFrame(df).
      encodeX("status", Ordinal).
      encodeY(field = "*", Quantitative, aggregate =
        AggOps.Count).
      mark(Bar).
      show

  }
}