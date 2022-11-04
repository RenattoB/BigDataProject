# Big Data Project of Stack OverFlow Posts

Main goal of this project is to obtain insights and key information about Posts most of the developers made through the platform Stack Overflow.
In order to obtain that insights the data will load, transform and analyzed through in and structured process.

## 1. Data Set Description
### 1.1 Source and Description

The project will use the Database of Stack OverFlow Posts. The database can be consult trough next link https://www.brentozar.com/archive/2015/10/how-to-download-the-stack-overflow-database-via-bittorrent/

#### Posts Structure
|        Column         | Description                                                                                                                                |
|:---------------------:|--------------------------------------------------------------------------------------------------------------------------------------------|
|          Id           | Id of the Post                                                                                                                             |
|      PostTypeId       | Id of the Post Type, is listed in the PostTypes table                                                                                      |
|   AcceptedAnswerId    | Only present if PostTypeId = 1                                                                                                             |
|       ParentId        | Only present if PostTypeId = 2                                                                                                             |
|     CreationDate      | Date time Post was created on                                                                                                              |
|         Score         | Score of the post.                                                                                                                         |
|       ViewCount       | Number of times the post has been seen.                                                                                                    |
|         Body          | HTML of the Post                                                                                                                           |
|      OwnerUserId      | Id of the owner of the post (only present if user has not been deleted; always -1 for tag wiki entries, i.e. the community user owns them) |
|         Body          | HTML of the Post                                                                                                                           |
|   OwnerDisplayName    | Name of the owner of the Post                                                                                                              |
|   LastEditorUserId    | If of the last editor of the Post                                                                                                          |
| LastEditorDisplayName | Name of the last editor of the Post                                                                                                        |
|     LastEditDate      | Date Time of the last edition of the Post                                                                                                  |
|   LastActivityDate    | Date Time of the last activity made in the Post                                                                                            |
|         Title         | Title of the Post                                                                                                                          ||       Body       | HTML of the Post                                                                                                                           ||       Body       | HTML of the Post                                                                                                                           ||       Body       | HTML of the Post                                                                                                                           ||       Body       | HTML of the Post                                                                                                                           ||       Body       | HTML of the Post                                                                                                                           ||       Body       | HTML of the Post                                                                                                                           ||       Body       | HTML of the Post                                                                                                                           ||       Body       | HTML of the Post                                                                                                                           |
|         Tags          | Tags the Posts has                                                                                                                         |
|      AnswerCount      | Number of Answers of the Post                                                                                                              |
|     CommentCount      | Number of Comments of the Post                                                                                                             |
|     FavoriteCount     | Number of times users put the Post with favorite mark                                                                                      |
|      ClosedDate       | Close Date of the Post                                                                                                                     ||       Body       | HTML of the Post                                                                                                                           |
|  CommunityOwnedDate   | Present only if post is community wiki'd                                                                                                   |


