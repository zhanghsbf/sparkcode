package controller

import service.WordCountService

object WordCountController extends BaseController {
  def main(args: Array[String]) = {
    run()
  }

  private val wordCountService = new WordCountService

  override def process(): Unit = {
    wordCountService.run()
  }
}
