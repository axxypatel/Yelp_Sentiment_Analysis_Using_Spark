import org.apache.spark.sql.DataFrame
import util.MLPreprocessing

import scala.util.Try

class MLPreprocessingSpec extends FunSpec with SparkSessionTestWrapper
  with DataFrameComparer{

  import spark.implicits._
  val reviews = Seq(("Call me crazy, but I have sold my soul to the Greeks. I had no idea how delicious Greek food can be. I think I'm in love!"),
    ("I went to Krasi on a Friday around 5 PM to secure a table. My party of 2 was seated at the bar since it was already starting to get busy at 5pm! The décor of the restaurant is beautiful and the half of the bar seats look directly at the chefs preparing the meals."),
    ("This was our least favourite of the evening but still packed a punch of flavour. Krasi is already a hot spot amongst the young professionals working in Back Bay. They don't take reservations so make sure to get there early if you hope to experience a Greek culinary adventure."),
    ("Great service, great food, and well-priced. Our server was super friendly and the girl who came around to make the tzatziki was absolutely amazing, super friendly, knowledgeable and actually Greek! "),
    ("F.I.N.A.L.L.Y GREEK FOOD & WINE get the focus they deserve!!! BRAVO!!!"),
    ("As soon as I walked in, I knew this place was special. The restaurant is gorgeous, modern and rich in detail. "),
    ("Wonderful food, wine, and service! We ordered the braised octopus, lamb osso buco, whipped feta, loukaniko, and octopus mortadella. Everything was delicious."),
    ("Impeccable experience from start to finish. From the 5 star service"),
    ("Disappointing. Only a couple of memorable dishes--osso bucco, steak tartar, and fish of the day. The octopus was way too salty, even for my salt-obsessed BF."),
    (" The manager was super accommodating to me and my friends as we came in on a very crowded night even through it was a Monday?")
  )
  val reviewDF = reviews.toDF("text")

  val actualDF = new MLPreprocessing(spark,reviewDF, false).reviewPreprocess()
  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  it("has features columns for ML model") {
    assert(hasColumn(actualDF, "features"))
  }

  it("has ngrams") {
    assert(hasColumn(actualDF, "ngrams"))
  }

  it("has processed all the rows") {
    assert(actualDF.count() == 10)
  }

  it("has features as vector datatype for model to read it correctly") {
    assert(actualDF.schema("features").dataType.toString == "org.apache.spark.ml.linalg.VectorUDT@3bfc3ba7")
  }
}