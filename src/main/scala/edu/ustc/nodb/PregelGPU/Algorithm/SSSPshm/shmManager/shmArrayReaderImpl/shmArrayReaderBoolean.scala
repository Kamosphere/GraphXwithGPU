package edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmArrayReaderImpl

import edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmArrayReader

class shmArrayReaderBoolean(preAllocateSize: Int, fullName: String)
  extends shmArrayReader(preAllocateSize){

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Boolean" + identifier)
  }

  var shmName : String = fullName

  type T = Boolean

  var dataTypeSize : Int = 1

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayReaderGet(): Array[Boolean] = {
    val t = buffer.asReadOnlyBuffer()
    val arrayTest = new Array[Byte](t.remaining())
    t.get(arrayTest)
    arrayTest.map(p => if (p == 1) true else false)

  }

}