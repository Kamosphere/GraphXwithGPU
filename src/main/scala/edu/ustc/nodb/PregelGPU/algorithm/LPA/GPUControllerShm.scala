package edu.ustc.nodb.PregelGPU.algorithm.LPA

import java.nio.file.{Files, Path, Paths}
import java.util

import edu.ustc.nodb.PregelGPU.algorithm.LPAPair
import edu.ustc.nodb.PregelGPU.envControl
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.graphx.util.collection.shmManager.shmArrayReaderImpl._
import org.apache.spark.graphx.util.collection.shmManager.shmNamePackager.{shmReaderPackager, shmWriterPackager}
import org.apache.spark.util.collection.BitSet

import scala.sys.process.Process

class GPUControllerShm(vertexSum: Long,
                       vertexCount: Int,
                       edgeCount: Int,
                       pid: Int) extends Serializable {

  // scalastyle:off println

  def this(pid: Int) = this(0, 0, 0, pid)

  // native interface
  val native = new GPUNativeShm

  System.loadLibrary("LPACPUShm")

  def GPUServerActive():
  Unit = {

    var runningScript = ""

    // running script to activate server
    if (envControl.controller == 0) {
      runningScript =
        "/usr/local/ssspexample/cpp_native/build/bin/srv_UtilServerTest_LabelPropagation " +
          vertexSum.toString + " " + edgeCount.toString + " " +
          1 + " " + pid.toString

    }
    else {
      runningScript =
        "./cpp_native/build/bin/srv_UtilServerTest_LabelPropagation " +
          vertexSum.toString + " " + edgeCount.toString + " " +
          1 + " " + pid.toString

    }

    Process(runningScript).run()

  }

  // before executing, run the server first
  def GPUEnvEdgeInit(filteredVertex: Array[Long],
                     EdgeSrc: String,
                     EdgeDst: String,
                     EdgeAttr: String):
  Unit = {

    GPUShutdown(0)

    GPUServerActive()

    // initialize the source id array
    val sourceId = new util.ArrayList[Long]

    // if native method not success, it will run until finished
    var result = false

    val shmReader = new shmReaderPackager(3)

    shmReader.addName(EdgeSrc, edgeCount)
    shmReader.addName(EdgeDst, edgeCount)
    shmReader.addName(EdgeAttr, edgeCount)

    while(! result) {
      result = native.nativeEnvEdgeInit(filteredVertex, vertexSum, sourceId, pid, shmReader)
    }
  }

  // execute SSSP algorithm
  def GPUMsgExecute(VertexID: String,
                    VertexActive: String,
                    VertexAttrKey: String,
                    VertexAttrValue: String,
                    modifiedVertexAmount: Int,
                    global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[LPAPair], Boolean) = {

    // create reader list to get input array in shm
    val shmReader = new shmReaderPackager(4)

    shmReader.addName(VertexID, modifiedVertexAmount)
    shmReader.addName(VertexActive, modifiedVertexAmount)
    shmReader.addName(VertexAttrKey, modifiedVertexAmount)
    shmReader.addName(VertexAttrValue, modifiedVertexAmount)

    // create writer list to return shm file names
    val shmWriter = new shmWriterPackager(3)

    // pass reader and writer through JNI
    var underIndex = native.nativeStepMsgExecute(vertexSum,
      shmReader, shmWriter,
      vertexCount, edgeCount, 0, pid)

    val needCombine = if (underIndex <= 0) false else true
    underIndex = math.abs(underIndex)

    // read files in writer list ( already written in c++ )
    val resultIDReader = new shmArrayReaderLong(
      shmWriter.getSizeByIndex(0), shmWriter.getNameByIndex(0))
    val resultAttrKeyReader = new shmArrayReaderLong(
      shmWriter.getSizeByIndex(1), shmWriter.getNameByIndex(1))
    val resultAttrValueReader = new shmArrayReaderLong(
      shmWriter.getSizeByIndex(2), shmWriter.getNameByIndex(2))

    val (resultBitSet, resultAttr) = vertexAttrPackage(
      underIndex, global2local, resultIDReader, resultAttrKeyReader, resultAttrValueReader)

    (resultBitSet, resultAttr, needCombine)

  }

  // execute algorithm while prev iter skipped
  def GPUIterSkipCollect(global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[LPAPair], Boolean) = {

    val shmWriter = new shmWriterPackager(3)

    // pass writer through JNI, for the data in the last iter is in the server
    var underIndex = native.nativeSkipStep(vertexSum,
      vertexCount, edgeCount, 0, pid,
      shmWriter)

    val needCombine = if (underIndex <= 0) false else true
    underIndex = math.abs(underIndex)

    val resultIDReader = new shmArrayReaderLong(
      shmWriter.getSizeByIndex(0), shmWriter.getNameByIndex(0))
    val resultAttrKeyReader = new shmArrayReaderLong(
      shmWriter.getSizeByIndex(1), shmWriter.getNameByIndex(1))
    val resultAttrValueReader = new shmArrayReaderLong(
      shmWriter.getSizeByIndex(2), shmWriter.getNameByIndex(2))

    val (resultBitSet, resultAttr) = vertexAttrPackage(
      underIndex, global2local, resultIDReader, resultAttrKeyReader, resultAttrValueReader)

    (resultBitSet, resultAttr, needCombine)

  }

  // execute algorithm in final step
  def GPUFinalCollect(global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[LPAPair], Boolean) = {

    val shmWriter = new shmWriterPackager(3)

    // pass writer through JNI, in order to get last skipped data back
    val underIndex = native.nativeStepFinal(vertexSum,
      vertexCount, edgeCount, 0, pid,
      shmWriter)

    val resultIDReader = new shmArrayReaderLong(
      shmWriter.getSizeByIndex(0), shmWriter.getNameByIndex(0))
    val resultAttrKeyReader = new shmArrayReaderLong(
      shmWriter.getSizeByIndex(1), shmWriter.getNameByIndex(1))
    val resultAttrValueReader = new shmArrayReaderLong(
      shmWriter.getSizeByIndex(2), shmWriter.getNameByIndex(2))

    val (resultBitSet, resultAttr) = vertexAttrPackage(
      underIndex, global2local, resultIDReader, resultAttrKeyReader, resultAttrValueReader)

    (resultBitSet, resultAttr, false)
  }

  // after executing, close the server and release the shared memory
  def GPUShutdown(runningStep: Int): Boolean = {

    // 0 for the first iter, other for the times of iter
    if (runningStep == 0) {

    }
    else {
      // remove all shm files in file way
      val k = Files.newDirectoryStream( Paths.get("/dev/shm/") ).iterator()
      var pathTemp: Path = null
      while(k.hasNext) {
        pathTemp = k.next()
        Files.deleteIfExists(pathTemp)

      }
    }
    native.nativeEnvClose(pid)

  }

  def vertexAttrPackage(underIndex: Int,
                        global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
                        resultIDReader: shmArrayReaderLong,
                        resultAttrKeyReader: shmArrayReaderLong,
                        resultAttrValueReader: shmArrayReaderLong):
  (BitSet, Array[LPAPair]) = {

    val resultBitSet = new BitSet(vertexCount)
    val resultAttr = new Array[LPAPair](vertexCount)

    //val startNew = System.nanoTime()
    var tempAttr : (VertexId, Long) = null
    for (i <- 0 until underIndex) {
      val globalId = resultIDReader.shmArrayReaderGetByIndex(i)
      tempAttr = (resultAttrKeyReader.shmArrayReaderGetByIndex(i),
        resultAttrValueReader.shmArrayReaderGetByIndex(i))

      val localId = global2local(globalId)

      if (resultBitSet.get(localId)) {
        resultAttr(localId). += (tempAttr)
      }
      else {
        resultBitSet.set(localId)
        resultAttr(localId) = Map[VertexId, Long](tempAttr)
      }
    }
    //val endNew = System.nanoTime()

    (resultBitSet, resultAttr)
  }

  // scalastyle:on println
}