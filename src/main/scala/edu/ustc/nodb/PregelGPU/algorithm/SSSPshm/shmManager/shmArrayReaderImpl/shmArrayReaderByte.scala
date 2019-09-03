package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayReaderImpl

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayReader

class shmArrayReaderByte(preAllocateSize: Int, fullName: String)
  extends shmArrayReader(preAllocateSize){

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Byte" + identifier)
  }

  var shmName : String = fullName

  type T = Byte

  var dataTypeSize : Int = 1


  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayReaderGet(): Array[Byte] = {
    val t = buffer.asReadOnlyBuffer()
    val arrayTest = new Array[Byte](t.remaining())
    t.get(arrayTest)
    arrayTest

  }

}