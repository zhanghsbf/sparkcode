package application

import common.ApplicationContext

trait BaseApplication {
  def process():Unit
  def run():Unit = {
    try{
      process()
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      ApplicationContext.close()
    }
  }
}
