package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayReaderImpl

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayReader

class shmArrayReaderChar(preAllocateSize: Int, fullName: String)
  extends shmArrayReader(preAllocateSize){

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Char" + identifier)
  }

  var shmName : String = fullName

  type T = Char

  var dataTypeSize : Int = 2

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayReaderGet(): Array[Char] = {
    val t = buffer.asCharBuffer()
    val arrayTest = new Array[Char](t.remaining())
    t.get(arrayTest)
    arrayTest

  }

}