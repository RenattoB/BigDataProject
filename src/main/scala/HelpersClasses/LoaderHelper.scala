package HelpersClasses

import org.apache.commons.math3.util.Precision.round
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, length, lit}

import scala.collection.mutable
import scala.reflect.runtime.{universe => runTimeUniverse}

object LoaderHelper {
  def colMatcher(optionalCols: Set[String],
                 mainDFCols: Set[String]): List[Column] = {
    mainDFCols.toList.map {
      case x if optionalCols.contains(x) => col(x)
      case x                             => lit(null).as(x)
    }
  }

  def getCaseClassType[T: runTimeUniverse.TypeTag]
  : List[runTimeUniverse.Symbol] = {
    runTimeUniverse.typeOf[T].members.toList
  }

  def getMembers[nameCaseClass: runTimeUniverse.TypeTag]: List[String] = {
    getCaseClassType[nameCaseClass]
      .filter(!_.isMethod)
      .map(x => x.name.decodedName.toString.replaceAll(" ", ""))

  }

  def readTable(spark: SparkSession, tableName:String): DataFrame = {
    spark.read
      .format("jdbc")
      .options(Map(
        "drive" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "url" -> "jdbc:sqlserver://localhost:1456;databaseName=StackOverflow2013",
        "user" -> "sa",
        "password" -> "sql@server2022",
        "dbtable" -> s"dbo.$tableName"
      ))
      .load()
  }

  def getPostsEda(dfPosts: DataFrame): String = {
    val returnString = new mutable.StringBuilder("\nEDA report for Posts Dataframe\n")
    val dfPostsTotal = dfPosts.count()
    val columnNames = dfPosts.columns
    val columnNumber = columnNames.length
    returnString.append(s"Numbers of rows -> $dfPostsTotal\n")
    returnString.append(s"Numbers of columns -> $columnNumber\n")
    var numberOfCellsInNull : Long = 0
    returnString.append("Column Report:\n")
    columnNames.foreach {columnName =>
      var columnNull : Long = 0
      if (dfPosts.schema(columnName).dataType.typeName == "timestamp") {
        columnNull = dfPosts.filter(dfPosts(columnName).isNull || dfPosts(columnName) === "").count()
      } else {
        columnNull = dfPosts.filter(dfPosts(columnName).isNull || dfPosts(columnName) === "" || dfPosts(columnName).isNaN).count()
      }
      numberOfCellsInNull += columnNull
      returnString.append(s"Number of $columnName in null -> $columnNull \n")
      returnString.append(s"Percentage of $columnName in null -> ${round(columnNull * 100 / dfPostsTotal.toFloat,3)} % \n")
    }
    returnString.append(s"\nNumber of cells in null $numberOfCellsInNull")
    returnString.append(s"\nPercentage of cells in null ${numberOfCellsInNull * 100 / (columnNumber * dfPostsTotal).toFloat}%\n")
    returnString.toString()
  }

  def generateParquetData(spark: SparkSession): Unit = {
    val postsDf = readTable(spark, "Posts")
    val usersDf = readTable(spark, "Users")
    val postsLinksDf = readTable(spark, "PostLinks")
    val commentsDf = readTable(spark, "Comments")
    val badgesDf = readTable(spark, "Badges")
    val postsTypesDf = readTable(spark, "PostTypes")
    val postsVotesDf = readTable(spark, "Votes")
    val postsVoteTypesDf = readTable(spark, "VoteTypes")

    //val postsDfSample = postsDf.sample(0.20)

    postsDf.write.parquet("src/main/resources/posts.parquet")
    usersDf.write.parquet("src/main/resources/users.parquet")
    postsLinksDf.write.parquet("src/main/resources/postLinks.parquet")
    commentsDf.write.parquet("src/main/resources/comments.parquet")
    badgesDf.write.parquet("src/main/resources/badges.parquet")
    postsTypesDf.write.parquet("src/main/resources/postTypes.parquet")
    postsVotesDf.write.parquet("src/main/resources/Votes.parquet")
    postsVoteTypesDf.write.parquet("src/main/resources/VoteTypes.parquet")
  }
}
