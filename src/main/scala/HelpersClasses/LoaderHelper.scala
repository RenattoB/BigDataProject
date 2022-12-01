package HelpersClasses

import org.apache.commons.math3.util.Precision
import org.apache.log4j.{BasicConfigurator, Level, LogManager, Logger}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{asc, avg, col, count, countDistinct, date_format, desc, expr, hour, lit, round, sum}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.collection.mutable
import scala.reflect.runtime.{universe => runTimeUniverse}

object LoaderHelper {
  BasicConfigurator.configure();
  val log: Logger = LogManager.getRootLogger
  val googleStorageOutput = "gs://dataflow-bckt/output/"
  log.setLevel(AppInfo.APP_INFO)

  def colMatcher(optionalCols: Set[String],
                 mainDFCols: Set[String]): List[Column] = {
    mainDFCols.toList.map {
      case x if optionalCols.contains(x) => col(x)
      case x => lit(null).as(x)
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

  def readTable(spark: SparkSession, tableName: String): DataFrame = {
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
    var numberOfCellsInNull: Long = 0
    returnString.append("Column Report:\n")
    columnNames.foreach { columnName =>
      var columnNull: Long = 0
      if (dfPosts.schema(columnName).dataType.typeName == "timestamp") {
        columnNull = dfPosts.filter(dfPosts(columnName).isNull || dfPosts(columnName) === "").count()
      } else {
        columnNull = dfPosts.filter(dfPosts(columnName).isNull || dfPosts(columnName) === "" || dfPosts(columnName).isNaN).count()
      }
      numberOfCellsInNull += columnNull
      returnString.append(s"Number of $columnName in null -> $columnNull \n")
      returnString.append(s"Percentage of $columnName in null -> ${Precision.round(columnNull * 100 / dfPostsTotal.toFloat, 3)} % \n")
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

  def generateInsights(dfPosts: DataFrame, postsDfClean: DataFrame, spark: SparkSession): Unit = {

    val usersDf = spark.read.parquet("src/main/resources/users.parquet")
    //val postsDf = spark.read.parquet("/opt/spark-data/posts.parquet")
    //val usersDf = spark.read.parquet("/opt/spark-data/users.parquet")

    //INSIGHTS
    val postsJoinUsers = postsDfClean.col("OwnerUserId") === usersDf.col("Id")
    //1.- Top 50 users with the highest average answer score excluding community wiki / closed posts
    val postsAndUsers = postsDfClean.join(usersDf, postsJoinUsers, "inner")
    val firstInsight = postsAndUsers
      .filter("PostTypeId = 2 and CommunityOwnedDate is null and ClosedDate is null")
      .groupBy(usersDf.col("Id"), usersDf.col("DisplayName"))
      .agg(
        countDistinct(dfPosts.col("Id")).as("Answers"),
        round(avg(col("Score")), 3).as("Average Answer Score")
      )
      .where("Answers > 20")
      .orderBy(desc("Average Answer Score"))
      .select(
        col("Id").as("User Id"),
        col("DisplayName").as("Username"),
        col("Answers"),
        col("Average Answer Score")
      )
      .limit(20)
    firstInsight.show()
    log.log(AppInfo.APP_INFO, "First insight start write\n")
    firstInsight.write.option("header","true").csv(googleStorageOutput + "firstInsight/")
    log.log(AppInfo.APP_INFO, "First insight finish write\n")

    //2.- Users with highest accept rate of their answers
    val postsParentDf = spark.read.parquet("src/main/resources/posts.parquet")
    //val postsParentDf = spark.read.parquet("/opt/spark-data/posts.parquet")
    val postsUsersJoinCondition = col("Posts.OwnerUserId") === col("Users.Id")
    val acceptanceDf = postsDfClean.as("Posts")
      .join(usersDf.as("Users"), postsUsersJoinCondition, "inner")
      .join(postsParentDf.as("PostParent"), col("PostParent.Id") === col("Posts.ParentId"), "inner")
      .filter("(PostParent.OwnerUserId != Users.Id or PostParent.OwnerUserId is null)")
      .withColumn("AcceptedAnswerFlag", expr("case when PostParent.AcceptedAnswerId = Posts.Id then 1 else 0 end"))

    val secondInsight = acceptanceDf
      .groupBy("Users.Id", "Users.DisplayName")
      .agg(
        count("Users.Id").as("Number of Answers"),
        sum("AcceptedAnswerFlag").as("Number Accepted"),
        round(sum("AcceptedAnswerFlag") * 100 / count("Posts.Id"), 3).as("Accepted Percent")
      )
      .orderBy(desc("Accepted Percent"), desc("Number of Answers"))
      .filter(col("Number of Answers") > 10)
      .limit(50)
    secondInsight.show()
    log.log(AppInfo.APP_INFO, "Second insight start write\n")
    secondInsight.write.option("header","true").csv(googleStorageOutput + "secondInsight/")
    log.log(AppInfo.APP_INFO, "Second insight finish write\n")

    //3.- Top Users by Number of Bounties Won
    val votesDf = spark.read.parquet("src/main/resources/Votes.parquet")
    //val votesDf = spark.read.parquet("/opt/spark-data/Votes.parquet")
    val votesPostsJoinCondition = col("Votes.PostId") === col("Posts.Id")
    val votesUsersPostsJoinDf = votesDf.as("Votes")
      .join(postsDfClean.as("Posts"), votesPostsJoinCondition, "inner")
      .join(usersDf.as("Users"), postsUsersJoinCondition, "inner")

    val thirdInsight = votesUsersPostsJoinDf
      .filter("Votes.VoteTypeId = 9")
      .groupBy("Posts.OwnerUserId", "Users.DisplayName")
      .agg(
        count("Votes.Id").as("Bounties Won")
      )
      .withColumnRenamed("DisplayName", "Username")
      .withColumnRenamed("OwnerUserId", "User Id")
      .orderBy(desc("Bounties Won"))
      .limit(10)
    thirdInsight.show(10)
    log.log(AppInfo.APP_INFO, "Third insight start write\n")
    thirdInsight.write.option("header","true").csv(googleStorageOutput + "thirdInsight/")
    log.log(AppInfo.APP_INFO, "Third insight finish write\n")

    //4.-  Most Upvoted Answers of All Time
    val votesPostsJoinDf = votesDf.as("Votes")
      .join(postsDfClean.as("Posts"), votesPostsJoinCondition, "inner")

    val fourthInsight = votesPostsJoinDf
      .filter("Posts.PostTypeId = 2 and Votes.VoteTypeId = 2")
      .groupBy("Votes.PostId")
      .agg(
        count("Votes.PostId").as("Vote Count")
      )
      .withColumnRenamed("PostId", "Post Id")
      .orderBy(desc("Vote Count"))
      .limit(20)

    fourthInsight.show()
    log.log(AppInfo.APP_INFO, "Fourth insight start write\n")
    fourthInsight.write.option("header","true").csv(googleStorageOutput + "fourthInsight/")
    log.log(AppInfo.APP_INFO, "Fourth insight finish write\n")

    //5.- Distribution of User Activity Per Hour
    val commentsDf = spark.read.parquet("src/main/resources/comments.parquet")
    //val commentsDf = spark.read.parquet("/opt/spark-data/comments.parquet")
    val hoursFromPostsDf = postsDfClean.select(hour(col("CreationDate")).as("Hour"), lit(1).as("Counter"))
    val hoursFromCommentsDf = commentsDf.select(hour(col("CreationDate")).as("Hour"), lit(1).as("Counter"))
    val distinctHoursFromPostDf = postsDfClean.select(hour(col("CreationDate")).as("Hour")).dropDuplicates()
    val fifthInsight = hoursFromPostsDf.union(hoursFromCommentsDf).as("HoursUnion")
      .join(distinctHoursFromPostDf.as("DistinctHours"), col("HoursUnion.Hour") === col("DistinctHours.Hour"), "right")
      .groupBy("DistinctHours.Hour")
      .agg(
        count("HoursUnion.Counter").as("Activity Traffic")
      )
      .orderBy(asc("DistinctHours.Hour"))
      .limit(24)

    fifthInsight.show()
    log.log(AppInfo.APP_INFO, "Fifth insight start write\n")
    fifthInsight.write.option("header","true").csv(googleStorageOutput + "fifthInsight/")
    log.log(AppInfo.APP_INFO, "Fifth insight finish write\n")

    //6.- Questions Count by Month
    val postsLinksDf = spark.read.parquet("src/main/resources/postLinks.parquet")
    //val postsLinksDf = spark.read.parquet("/opt/spark-data/postLinks.parquet")
    val sixthInsight = postsDfClean.as("Posts")
      .join(postsLinksDf.as("PostsLinks"), col("Posts.Id") === col("PostsLinks.PostId"), "left")
      .join(postsParentDf.as("PostParent"), col("PostParent.Id") === col("PostsLinks.RelatedPostId"), "left")
      .where("Posts.PostTypeId = 1")
      .select(
        date_format(col("Posts.CreationDate"), "yyyy-MM").as("Date"),
        col("Posts.Id")
      )
      .groupBy("Date")
      .agg(
        count("Posts.Id").as("Quantity")
      )
      .orderBy(desc("Date"))

    sixthInsight.show()
    log.log(AppInfo.APP_INFO, "Sixth insight start write\n")
    sixthInsight.repartition(1).write.option("header","true").csv(googleStorageOutput + "sixthInsight/")
    log.log(AppInfo.APP_INFO, "Fifth insight finish write\n")


  }

  def getOutliers(dfPosts: DataFrame, spark: SparkSession): DataFrame = {
    val schema = StructType(Seq(
      StructField("Id", IntegerType, nullable = false)
    ))
    var dfId: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    dfPosts.columns.foreach { columnName =>
      if ((columnName == "Score" || columnName == "ViewCount" || columnName == "CommentCount") && dfPosts.schema(columnName).dataType.typeName == "integer") {
        val strBuilder = new mutable.StringBuilder(s"\nOutliers for column -> $columnName\n")

        val Array(q1, q3) = dfPosts.stat.approxQuantile(columnName,
          Array(0.25, 0.75), 0.0)
        val iqr = q3 - q1
        val upBoundary = q3 + 1.5 * iqr
        val lowBoundary = q1 - 1.5 * iqr

        strBuilder.append(s"Lower Boundary: $lowBoundary\n")
        strBuilder.append(s"Upper Boundary: $upBoundary\n")
        strBuilder.append(s"Inter quartile: $iqr\n")

        val outliers = dfPosts.filter(col(columnName) < lowBoundary || col(columnName) > upBoundary).select("Id")

        strBuilder.append(s"Quantity of outliers values for $columnName column -> : ${outliers.count()}\n")
        dfId = dfId.union(outliers)
        log.log(AppInfo.APP_INFO,strBuilder)
      }
    }
    dfId.distinct()
  }

}
