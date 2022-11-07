package HelpersClasses
import org.apache.log4j.Level

class AppInfo extends Level(Level.FATAL.toInt, "APP_INFO", 0)

object AppInfo {
  final val APP_INFO = new AppInfo()
}