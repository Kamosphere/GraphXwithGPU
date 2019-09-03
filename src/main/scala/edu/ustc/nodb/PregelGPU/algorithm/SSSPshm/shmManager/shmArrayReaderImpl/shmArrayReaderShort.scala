package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayReaderImpl

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayReader

class shmArrayReaderShort(preAllocateSize: Int, fullName: String)
  extends shmArrayReader(preAllocateSize){

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Short" + identifier)
  }

  var shmName : String = fullName

  type T = Short

  var dataTypeSize : Int = 2

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayReaderGet(): Array[Short] = {
    val t = buffer.asShortBuffer()
    val arrayTest = new Array[Short](t.remaining())
    t.get(arrayTest)
    arrayTest

  }

}