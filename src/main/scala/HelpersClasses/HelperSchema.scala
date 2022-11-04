package HelpersClasses

object HelperSchema {

  case class PostData(
                       acceptedAnswerId: Option[Long],
                       answerCount: Option[Long],
                       body: Option[String],
                       closedDate: Option[String],
                       commentCount: Option[Long],
                       communityOwnedDate: Option[String],
                       creationDate: Option[String],
                       favoriteCount: Option[Long],
                       id: Option[Long],
                       lastActivityDate: Option[String],
                       lastEditDate: Option[String],
                       lastEditorUserId: Option[Long],
                       ownerDisplayName: Option[String],
                       ownerUserId: Option[Long],
                       parentId: Option[Long],
                       postTypeId: Option[Long],
                       score: Option[Long],
                       tags: Option[String],
                       title: Option[String],
                       value: Option[String],
                       viewCount: Option[Long]
                     )

}
