package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager

import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

abstract class shmArrayPrototype(preAllocateSize: Int) {

  type T

  var dataTypeSize : Int

  var shmName : String

  val channel : FileChannel
  val buffer : MappedByteBuffer

}
