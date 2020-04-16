import batch.CalculateScore
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class CalculateScoreSpec extends FunSpec with SparkSessionTestWrapper
  with DataFrameComparer{

  import spark.implicits._
  val reviews = Seq(("111", "review1", 23, "1.0"),
    ("111", "review2", 35, "1.0"),
    ("111", "review3", 76, "0.0"),
    ("222", "review4", 42, "0.0"),
    ("222", "review5", 4,"0.0"),
    ("222", "review6", 13,"1.0"),
    ("333", "review7", 55, "0.0"),
    ("333", "review8", 987, "0.0"),
    ("444", "review9", 1,"1.0"),
    ("444", "review10", 5,"1.0")
  )
  val reviewDF = reviews.toDF("business_id", "review", "useful", "sentiment")

  val method = new CalculateScore;
  val actualDF = method.createDFWithScore(spark, reviewDF)
  val listScores:List[Double] = actualDF.select("score").collect().map(_.getDouble(0)).toList

  it("get scores for all business_ids") {
    assert(actualDF.count() == 4)
  }

  it("score cannot be less than 0") {
    val violatedScores = listScores.filter(_ < 0)
    assert(violatedScores.size == 0)
  }

  it("score cannot be more than 1") {
    val violatedScores = listScores.filter(_ > 1)
    assert(violatedScores.size == 0)
  }

  it("values are incorrectly calculated/classified") {
    val expected = Seq(Row("444", 1.0, Array("review10", "review9"), null),
      Row("111", 0.6666666666666666, Array("review2", "review1"), Array("review3")),
      Row("222", 0.3333333333333333, Array("review6"), Array("review5", "review4")),
      Row("333", 0.0, null, Array("review8", "review7")))

    val simpleSchema = StructType(Array(
      StructField("business_id",StringType,true),
      StructField("score",DoubleType,true),
      StructField("collect_set(review)", ArrayType(StringType,true), true),
      StructField("collect_set(review)", ArrayType(StringType,true), true)))
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expected), simpleSchema)
    assertSmallDataFrameEquality(actualDF, expectedDF)
  }
}