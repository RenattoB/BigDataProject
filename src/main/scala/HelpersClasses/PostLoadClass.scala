package HelpersClasses

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, avg, col, count, countDistinct, desc, expr, hour, lit, sum, date_format}

object PostLoadClass extends App{

  val sparkConf = new SparkConf()
    .setAppName("stackExchange-spark-analyzer")
    .set("spark.driver.allowMultipleContexts", "true")

  val spark =
    SparkSession
      .builder()
      .config(sparkConf)
      .master("local[*]")
      .getOrCreate()

  //reading and writing data
  //LoaderHelper.generateParquetData(spark)


  val postsDf = spark.read.parquet("src/main/resources/posts.parquet")
  val usersDf = spark.read.parquet("src/main/resources/users.parquet")



  //EDA REPORT
  print(LoaderHelper.getPostsEda(postsDf))

  //DATA CLEANING
  val postsDfClean = postsDf.na.fill("unknown", Array("LastEditorDisplayName"))
                            .na.fill("unknown", Array("Tags"))
                            .na.fill("unknown", Array("Title"))
  print(LoaderHelper.getPostsEda(postsDfClean))


  //INSIGHTS
  val postsJoinUsers = postsDfClean.col("OwnerUserId") === usersDf.col("Id")
  //1.- Top 50 users with the highest average answer score excluding community wiki / closed posts or users with less than 10 answers
  val postsAndUsers = postsDfClean.join(usersDf, postsJoinUsers, "inner")
  postsAndUsers
    .filter("PostTypeId = 2 and CommunityOwnedDate is null and ClosedDate is null")
    .groupBy(usersDf.col("Id"), usersDf.col("DisplayName"))
    .agg(
      countDistinct(postsDf.col("Id")).as("Answers"),
      avg(col("Score")).as("Average Answer Score")
    )
    .where("Answers > 20")
    .orderBy(desc("Average Answer Score"))
    .select(
      col("Id").as("User Id"),
      col("DisplayName").as("Username"),
      col("Answers"),
      col("Average Answer Score")
    )
    .limit(50)
    .show()

  //2.- Users with highest accept rate of their answers
  val postsParentDf = spark.read.parquet("src/main/resources/posts.parquet")
  val postsUsersJoinCondition = col("Posts.OwnerUserId") === col("Users.Id")
  val acceptanceDf = postsDfClean.as("Posts")
                    .join(usersDf.as("Users"), postsUsersJoinCondition, "inner")
                    .join(postsParentDf.as("PostParent"), col("PostParent.Id") === col("Posts.ParentId"), "inner")
                    .filter("(PostParent.OwnerUserId != Users.Id or PostParent.OwnerUserId is null)")
                    .withColumn("AcceptedAnswerFlag", expr("case when PostParent.AcceptedAnswerId = Posts.Id then 1 else 0 end"))

  acceptanceDf
    .groupBy("Users.Id", "Users.DisplayName")
    .agg(
      count("Users.Id").as("Number of Answers"),
      sum("AcceptedAnswerFlag").as("Number Accepted"),
      (sum("AcceptedAnswerFlag") * 100 / count("Posts.Id")).as("Accepted Percent")
    )
    .orderBy(desc("Accepted Percent"), desc("Number of Answers"))
    .filter(col("Number of Answers") > 10)
    .limit(50)
    .show()

  //3.- Top Users by Number of Bounties Won
  val votesDf = spark.read.parquet("src/main/resources/Votes.parquet")
  val votesPostsJoinCondition = col("Votes.PostId") === col("Posts.Id")
  val votesUsersPostsJoinDf = votesDf.as("Votes")
    .join(postsDfClean.as("Posts"), votesPostsJoinCondition, "inner")
    .join(usersDf.as("Users"), postsUsersJoinCondition, "inner")

  votesUsersPostsJoinDf
    .filter("Votes.VoteTypeId = 9")
    .groupBy("Posts.OwnerUserId", "Users.DisplayName")
    .agg(
      count("Votes.Id").as("Bounties Won")
    )
    .withColumnRenamed("DisplayName", "Username")
    .withColumnRenamed("OwnerUserId", "User Id")
    .orderBy(desc("Bounties Won"))
    .show()

  //4.-  Most Upvoted Answers of All Time
  val votesPostsJoinDf = votesDf.as("Votes")
    .join(postsDfClean.as("Posts"), votesPostsJoinCondition, "inner")

  votesPostsJoinDf
    .filter("Posts.PostTypeId = 2 and Votes.VoteTypeId = 2")
    .groupBy("Votes.PostId", "Posts.Body")
    .agg(
      count("Votes.PostId").as("Vote Count")
    )
    .withColumnRenamed("Body", "Question")
    .withColumnRenamed("PostId", "Post Id")
    .orderBy(desc("Vote Count"))
    .show()

  //5.- Distribution of User Activity Per Hour
  val commentsDf = spark.read.parquet("src/main/resources/comments.parquet")
  val hoursFromPostsDf = postsDfClean.select(hour(col("CreationDate")).as("Hour"), lit(1).as("Counter"))
  val hoursFromCommentsDf = commentsDf.select(hour(col("CreationDate")).as("Hour"), lit(1).as("Counter"))
  val distinctHoursFromPostDf = postsDfClean.select(hour(col("CreationDate")).as("Hour")).dropDuplicates()
  hoursFromPostsDf.union(hoursFromCommentsDf).as("HoursUnion")
    .join(distinctHoursFromPostDf.as("DistinctHours"), col("HoursUnion.Hour") === col("DistinctHours.Hour"), "right")
    .groupBy("DistinctHours.Hour")
    .agg(
      count("HoursUnion.Counter").as("Activity Traffic")
    )
    .orderBy(asc("DistinctHours.Hour"))
    .show(24)

  //6.- Questions Count by Month
  val postsLinksDf = spark.read.parquet("src/main/resources/postLinks.parquet")
  postsDfClean.as("Posts")
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
    .show()

}
