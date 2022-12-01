import HelpersClasses.{AppInfo, LoaderHelper}
import HelpersClasses.LoaderHelper.getPostsEda
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{BasicConfigurator, Level, LogManager, Logger}
import org.apache.spark.sql.functions.col

object Main {
  def main(args: Array[String]): Unit = {

    BasicConfigurator.configure()
    val log = LogManager.getRootLogger
    log.setLevel(AppInfo.APP_INFO)



    val sparkConf = new SparkConf()
      .setAppName("BigDataProject")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "src/main/resources/data-storage-key.json")



    val spark =
      SparkSession
        .builder()
        .config(sparkConf)
        .master("local[*]")
        .getOrCreate()

    //reading and writing data
    //LoaderHelper.generateParquetData(spark)


    val postsDf = spark.read.parquet("src/main/resources/posts.parquet")
    postsDf.show(10)
    postsDf.describe()
    log.log(AppInfo.APP_INFO, "Posts Schema:\n")
    postsDf.printSchema()

    /*log.log(AppInfo.APP_INFO, "Start save\n")
    postsDf.write.option("header","true").csv(googleStorageOutput + "ekisde/")
    log.log(AppInfo.APP_INFO, "Finish save\n")*/




    //EDA REPORT
    log.log(AppInfo.APP_INFO, getPostsEda(postsDf))

    //DATA CLEANING
    val dfPostsClean = postsDf.na.fill("unknown", Array("LastEditorDisplayName"))
      .na.fill("unknown", Array("Tags"))
      .na.fill("unknown", Array("Title"))
    //val idOutliers = LoaderHelper.getOutliers(dfPostsClean, spark)
    /*val postsWithoutOutliers = dfPostsClean.as("dfPosts")
      .join(idOutliers.as("dfOutliers"), col("dfPosts.Id") === col("dfOutliers.Id"), "left_anti")*/
    log.log(AppInfo.APP_INFO, getPostsEda(dfPostsClean))

    LoaderHelper.generateInsights(postsDf, dfPostsClean, spark)


  }
}
