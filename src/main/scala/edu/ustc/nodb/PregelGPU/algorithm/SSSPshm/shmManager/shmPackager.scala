package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager

abstract class shmPackager(maxSize: Int){

  val shmNameArr : Array[String] = new Array[String](maxSize)
  val shmSizeArr : Array[Int] = new Array[Int](maxSize)

  var pointer : Int = 0
  var isFull : Boolean = false

  def addName(shmName: String, counter: Int): Boolean

  def getNameByUnder(underScore: Int): String

  def getSizeByUnder(underScore: Int): Int
}
