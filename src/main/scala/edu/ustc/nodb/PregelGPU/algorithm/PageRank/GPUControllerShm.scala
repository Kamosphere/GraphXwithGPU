package edu.ustc.nodb.PregelGPU.algorithm.PageRank

import java.nio.file.{Files, Path, Paths}
import java.util

import edu.ustc.nodb.PregelGPU.algorithm.SPMap
import edu.ustc.nodb.PregelGPU.envControl
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.graphx.util.collection.shmManager.shmArrayReaderImpl.{shmArrayReaderDouble, shmArrayReaderLong}
import org.apache.spark.graphx.util.collection.shmManager.shmNamePackager.{shmReaderPackager, shmWriterPackager}
import org.apache.spark.util.collection.BitSet

import scala.collection.mutable.ArrayBuffer
import scala.sys.process.Process

class GPUControllerShm(vertexSum: Long,
                       vertexCount: Int,
                       edgeCount: Int,
                       source: VertexId,
                       pid: Int) extends Serializable {

  // scalastyle:off println

  def this(pid: Int) = this(0, 0, 0, -1L, pid)

  // native interface
  val native = new GPUNativeShm

  val sourceSize: Int = 1

  System.loadLibrary("PRCPUShm")

  def GPUServerActive():
  Unit = {

    var runningScript = ""

    // running script to activate server
    if (envControl.controller == 0) {
      runningScript =
        "/usr/local/ssspexample/cpp_native/build/bin/srv_UtilServerTest_PageRank " +
          vertexSum.toString + " " + edgeCount.toString + " " +
          1 + " " + pid.toString

    }
    else {
      runningScript =
        "./cpp_native/build/bin/srv_UtilServerTest_PageRank " +
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

    sourceId.add(source)

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
                    VertexAttrPair1: String,
                    VertexAttrPair2: String,
                    modifiedVertexAmount: Int,
                    global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[Double], Boolean) = {

    // create reader list to get input array in shm
    val shmReader = new shmReaderPackager(4)

    shmReader.addName(VertexID, modifiedVertexAmount)
    shmReader.addName(VertexActive, modifiedVertexAmount)
    shmReader.addName(VertexAttrPair1, modifiedVertexAmount * sourceSize)
    shmReader.addName(VertexAttrPair2, modifiedVertexAmount * sourceSize)

    // create writer list to return shm file names
    val shmWriter = new shmWriterPackager(2)

    // pass reader and writer through JNI
    var underIndex = native.nativeStepMsgExecute(vertexSum,
      shmReader, shmWriter,
      vertexCount, edgeCount, sourceSize, pid)

    val needCombine = if (underIndex <= 0) false else true
    underIndex = math.abs(underIndex)

    // read files in writer list ( already written in c++ )
    val resultIDReader = new shmArrayReaderLong(
      shmWriter.getSizeByIndex(0), shmWriter.getNameByIndex(0))
    val resultAttrReader = new shmArrayReaderDouble(
      shmWriter.getSizeByIndex(1), shmWriter.getNameByIndex(1))

    val (resultBitSet, resultAttr) = vertexAttrPackage(
      underIndex, global2local, resultIDReader, resultAttrReader)

    (resultBitSet, resultAttr, needCombine)

  }

  // execute algorithm while prev iter skipped
  def GPUIterSkipCollect(global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[Double], Boolean) = {

    val shmWriter = new shmWriterPackager(2)

    // pass writer through JNI, for the data in the last iter is in the server
    var underIndex = native.nativeSkipStep(vertexSum,
      vertexCount, edgeCount, sourceSize, pid,
      shmWriter)

    val needCombine = if (underIndex <= 0) false else true
    underIndex = math.abs(underIndex)

    val resultIDReader = new shmArrayReaderLong(
      shmWriter.getSizeByIndex(0), shmWriter.getNameByIndex(0))
    val resultAttrReader = new shmArrayReaderDouble(
      shmWriter.getSizeByIndex(1), shmWriter.getNameByIndex(1))

    val (resultBitSet, resultAttr) = vertexAttrPackage(
      underIndex, global2local, resultIDReader, resultAttrReader)

    (resultBitSet, resultAttr, needCombine)

  }

  // execute algorithm in final step
  def GPUFinalCollect(global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[Double], Boolean) = {

    val shmWriter = new shmWriterPackager(2)

    // pass writer through JNI, in order to get last skipped data back
    val underIndex = native.nativeStepFinal(vertexSum,
      vertexCount, edgeCount, sourceSize, pid,
      shmWriter)

    val resultIDReader = new shmArrayReaderLong(
      shmWriter.getSizeByIndex(0), shmWriter.getNameByIndex(0))
    val resultAttrReader = new shmArrayReaderDouble(
      shmWriter.getSizeByIndex(1), shmWriter.getNameByIndex(1))

    val (resultBitSet, resultAttr) = vertexAttrPackage(
      underIndex, global2local, resultIDReader, resultAttrReader)

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
                        resultAttrReader: shmArrayReaderDouble):
  (BitSet, Array[Double]) = {

    val resultBitSet = new BitSet(vertexCount)
    val resultAttr = new Array[Double](vertexCount)

    //val startNew = System.nanoTime()
    var tempAttr : Double = 0.0
    for (i <- 0 until underIndex) {
      val globalId = resultIDReader.shmArrayReaderGetByIndex(i)
      tempAttr = resultAttrReader.shmArrayReaderGetByIndex(i)
      val localId = global2local(globalId)
      resultBitSet.set(localId)
      resultAttr(localId) = tempAttr
    }
    //val endNew = System.nanoTime()

    (resultBitSet, resultAttr)
  }

  // scalastyle:on println
}
