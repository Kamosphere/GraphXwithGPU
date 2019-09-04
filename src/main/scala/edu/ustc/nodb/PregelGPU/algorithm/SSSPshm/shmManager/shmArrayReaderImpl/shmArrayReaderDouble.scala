package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayReaderImpl

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayReader

class shmArrayReaderDouble(preAllocateSize: Int, fullName: String)
  extends shmArrayReader(preAllocateSize) {

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Double" + identifier)
  }

  var shmName : String = fullName

  type T = Double

  var dataTypeSize : Int = 8

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayReaderGet(): Array[Double] = {
    val t = buffer.asDoubleBuffer()
    val arrayTest = new Array[Double](t.remaining())
    t.get(arrayTest)
    arrayTest

  }

}
