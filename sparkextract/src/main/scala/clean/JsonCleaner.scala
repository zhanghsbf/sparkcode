package clean

import scala.util.parsing.json.JSON

class JsonCleaner{
  def clean(str:String) = {
    JSON.parseRaw(str) match {
      case Some(s) => true
      case None => false
    }
  }
}
