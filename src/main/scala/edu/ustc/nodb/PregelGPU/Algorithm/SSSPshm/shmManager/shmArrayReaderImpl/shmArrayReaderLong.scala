package edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmArrayReaderImpl

import edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmArrayReader

class shmArrayReaderLong(preAllocateSize: Int, fullName: String)
  extends shmArrayReader(preAllocateSize){

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Long" + identifier)
  }

  var shmName : String = fullName

  type T = Long

  var dataTypeSize : Int = 8

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayReaderGet(): Array[Long] = {
    val t = buffer.asLongBuffer()
    val arrayTest = new Array[Long](t.remaining())
    t.get(arrayTest)
    arrayTest

  }

}