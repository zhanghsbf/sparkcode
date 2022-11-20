package controller

import common.ApplicationContext

trait BaseController {
  def process():Unit
  def run():Unit = {
    try{
      process()
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      ApplicationContext.session.close()
    }
  }
}
