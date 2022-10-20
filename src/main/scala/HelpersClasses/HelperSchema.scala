package HelpersClasses

object HelperSchema {

  case class PostData(
                       acceptedAnswerId: Long,
                       answerCount: Long,
                       body: String,
                       closedDate: String,
                       commentCount: Long,
                       communityOwnedDate: String,
                       creationDate: String,
                       favoriteCount: Long,
                       id: Long,
                       lastActivityDate: String,
                       lastEditDate: String,
                       lastEditorUserId: Long,
                       ownerDisplayName: String,
                       ownerUserId: Long,
                       parentId: Long,
                       postTypeId: Long,
                       score: Long,
                       tags: String,
                       title: String,
                       value: String,
                       viewCount: Long
                     )

}
