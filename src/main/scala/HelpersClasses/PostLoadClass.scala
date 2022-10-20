package HelpersClasses

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import HelperSchema.PostData

object PostLoadClass extends App{

  val postXmlPath = "src/main/scala/resources/Posts.xml"

  val sparkConf = new SparkConf()
    .setAppName("stackExchange-spark-analyzer")
    .set("spark.driver.allowMultipleContexts", "true")

  val spark =
    SparkSession
      .builder()
      .config(sparkConf)
      .master("local[*]")
      .getOrCreate()

  val postRawDF = spark.read
    .option("rowTag", "posts")
    .format("xml")
    .load(postXmlPath)

  val explodedMappedPostData = postRawDF
    .select(explode(col("row")))
    .select(
      LoaderHelper
        .getMembers[PostData]
        .map(x => col("col._" + x)): _*)

  val postDataset: Dataset[PostData] =
    LoaderHelper
      .removeSpecialCharsFromCols(explodedMappedPostData, "_", "")
      .as[PostData](Encoders.product)
      .cache()

  println(postDataset.show)
}
