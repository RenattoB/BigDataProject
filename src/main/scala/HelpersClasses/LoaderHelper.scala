package HelpersClasses

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}
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

  def removeSpecialCharsFromCols(
                                  data: DataFrame,
                                  replace: String,
                                  replaceWith: String): DataFrame= {
    data.columns.foldLeft(data) { (renamedDf, colname) =>
      renamedDf
        .withColumnRenamed(
          colname,
          colname.replace(replace, replaceWith))
    }
  }
}
