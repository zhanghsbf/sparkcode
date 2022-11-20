package controller

import service.EventService

object GameLogExtractController extends BaseController {
  private val service = new EventService
  var partDt = "9999-12-31"
  var gameLogDir = "D:\\CodePlace\\sparkextract\\data\\part_date=test"

  override def process(): Unit = {
    service.extract2(gameLogDir, partDt)
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("按顺序传入两个参数: 日志路径, 数据日期(yyyy-MM-dd")
      System.exit(1)
    }

    gameLogDir = args(0)
    partDt = args(1)
    run()
  }
}
