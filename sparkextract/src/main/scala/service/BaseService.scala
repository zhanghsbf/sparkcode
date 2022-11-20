package service

trait BaseService {
  def init(): Unit = {}

  def process(): Unit

  def destroy(): Unit = {}

  def run():Unit = {
    val start:Long = System.currentTimeMillis()
    init()
    val initCost: Long = System.currentTimeMillis() - start

    process()
    val processCost:Long = System.currentTimeMillis() - start - initCost

    destroy()
    val destroyCost:Long = System.currentTimeMillis() - start - initCost
  }
}
