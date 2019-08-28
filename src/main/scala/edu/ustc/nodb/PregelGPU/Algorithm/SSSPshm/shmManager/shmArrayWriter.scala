package edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager

import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}

abstract class shmArrayWriter(preAllocateSize: Int)
  extends shmArrayPrototype(preAllocateSize) {

  var dataTypeSize : Int

  var shmName : String

  lazy val channel : FileChannel = Files.newByteChannel(Paths.get("/dev/shm/" + shmName),
    StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
    .asInstanceOf[FileChannel]
  lazy val buffer : MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, preAllocateSize * dataTypeSize)


  def shmWriterClose(): String = {

    // write the remained data
    buffer.force()
    if(channel.isOpen){
      channel.close()
    }

    shmName
  }

  def shmArrayWriterSet(data: T): Unit
}
