package edu.ustc.nodb.PregelGPU.Algorithm.SSSP

import java.util

import edu.ustc.nodb.PregelGPU.Algorithm.SPMapWithActive
import edu.ustc.nodb.PregelGPU.envControl
import org.apache.spark.graphx.VertexId

import scala.collection.mutable.ArrayBuffer
import scala.sys.process.Process

class GPUNative extends Serializable {

  /* the GPU-based method to execute the SSSP algorithm
  possibly modified for template */

  // native function to init the edge
  @native def GPUServerInit(filteredVertex: Array[Long],
                            vertexSum: Long,
                            EdgeSrc: Array[VertexId],
                            EdgeDst: Array[VertexId],
                            EdgeAttr: Array[Double],
                            sourceId: util.ArrayList[Long],
                            pid:Int):
  Boolean

  // native function to execute algorithm for final step
  @native def GPUClientAllStep(vertexSum: Long,
                            vertexCount:Int,
                            edgeSize: Int,
                            markIdSize: Int,
                            pid: Int,
                            resultID: Array[Long],
                            resultAttr: Array[Double]):
  Int

  // native function to execute algorithm while prev iter skipped
  @native def GPUClientSkippedStep(vertexSum: Long,
                            vertexCount:Int,
                            edgeSize: Int,
                            markIdSize: Int,
                            pid: Int,
                            resultID: Array[Long],
                            resultAttr: Array[Double]):
  Int

  // native function to execute algorithm
  @native def GPUClientStep(vertexSum: Long,
                            VertexID: Array[Long],
                            VertexActive: Array[Boolean],
                            VertexAttr: Array[Double],
                            vertexCount:Int,
                            edgeCount: Int,
                            markIdSize: Int,
                            pid: Int,
                            resultID: Array[Long],
                            resultAttr: Array[Double]):
  Int

  // native function to close server
  @native def GPUServerShutdown(pid: Int):
  Boolean

  // execute algorithm in final step
  def GPUAllProcess(vertexSum: Long,
                    vertexCount: Int,
                    edgeSize: Int,
                    sourceList: ArrayBuffer[VertexId],
                    pid: Int):
  ArrayBuffer[(VertexId, SPMapWithActive)] = {

    System.loadLibrary("PregelGPU")

    val sourceSize = sourceList.length

    val resultID : Array[Long] = new Array[Long](vertexSum.toInt)
    val resultAttr : Array[Double] = new Array[Double](vertexSum.toInt * sourceSize)

    val results = new ArrayBuffer[(VertexId, SPMapWithActive)]

    var tempVertexSet : VertexSet = null

    //pass vertices through JNI and get arrayBuffer back
    val underIndex = GPUClientAllStep(vertexSum,
      vertexCount, edgeSize, sourceSize, pid,
      resultID, resultAttr)

    val startNew = System.nanoTime()

    for(i <- 0 until underIndex){

      // package the vertex as vertex-like format
      tempVertexSet = new VertexSet(resultID(i), false)
      for(j <- sourceList.indices){
        tempVertexSet.addAttr(sourceList(j), resultAttr(i * sourceSize + j))
      }

      results.+=(tempVertexSet.TupleReturn())
    }

    val endNew = System.nanoTime()
    println("Constructing remained arrayBuffer time: " + (endNew - startNew))
    results
  }

  // execute algorithm while prev iter skipped
  def GPUSkippedProcess(vertexSum: Long,
                        vertexCount: Int,
                        edgeSize: Int,
                        sourceList: ArrayBuffer[VertexId],
                        pid: Int):
  (ArrayBuffer[(VertexId, SPMapWithActive)], Boolean) = {

    System.loadLibrary("PregelGPU")

    val sourceSize = sourceList.length

    val resultID : Array[Long] = new Array[Long](vertexSum.toInt)
    val resultAttr : Array[Double] = new Array[Double](vertexSum.toInt * sourceSize)

    val results = new ArrayBuffer[(VertexId, SPMapWithActive)]

    var tempVertexSet : VertexSet = null

    //pass vertices through JNI and get arrayBuffer back
    var underIndex = GPUClientSkippedStep(vertexSum,
      vertexCount, edgeSize, sourceSize, pid,
      resultID, resultAttr)

    val needCombine = if(underIndex <= 0) false else true
    underIndex = math.abs(underIndex)

    val startNew = System.nanoTime()

    for(i <- 0 until underIndex){
      // package the message-like vertex
      tempVertexSet = new VertexSet(resultID(i), true)
      for(j <- sourceList.indices){
        tempVertexSet.addAttr(sourceList(j), resultAttr(i * sourceSize + j))
      }

      results.+=(tempVertexSet.TupleReturn())
    }

    val endNew = System.nanoTime()
    println("Constructing returned skipped arrayBuffer time: " + (endNew - startNew))
    (results, needCombine)

  }

  // execute SSSP algorithm
  def GPUProcess(VertexID: Array[Long],
                 VertexActive: Array[Boolean],
                 VertexAttr: Array[Double],
                 vertexSum: Long,
                 vertexCount: Int,
                 edgeSize: Int,
                 sourceList: ArrayBuffer[VertexId],
                 pid: Int):
  (ArrayBuffer[(VertexId, SPMapWithActive)], Boolean) = {

    System.loadLibrary("PregelGPU")

    val sourceSize = sourceList.length

    val resultID : Array[Long] = new Array[Long](vertexSum.toInt)
    val resultAttr : Array[Double] = new Array[Double](vertexSum.toInt * sourceSize)

    val results = new ArrayBuffer[(VertexId, SPMapWithActive)]

    var tempVertexSet : VertexSet = null

    // pass vertices through JNI and get result array back
    var underIndex = GPUClientStep(vertexSum,
      VertexID, VertexActive, VertexAttr,
      vertexCount, edgeSize, sourceSize, pid,
      resultID, resultAttr)

    val needCombine = if(underIndex <= 0) false else true
    underIndex = math.abs(underIndex)

    val startNew = System.nanoTime()

    for(i <- 0 until underIndex){
      // package the message-like vertex
      tempVertexSet = new VertexSet(resultID(i), true)
      var invalidDetector = 0.0
      for(j <- sourceList.indices){
        invalidDetector = resultAttr(i * sourceSize + j)
        if(invalidDetector < Int.MaxValue) tempVertexSet.addAttr(sourceList(j), resultAttr(i * sourceSize + j))
      }

      results.+=(tempVertexSet.TupleReturn())
    }

    val endNew = System.nanoTime()
    println("Constructing returned arrayBuffer time: " + (endNew - startNew))
    (results, needCombine)

  }

  // before executing, run the server first
  def GPUInit(vertexSum: Long,
              filteredVertex: Array[Long],
              EdgeSrc: Array[VertexId],
              EdgeDst: Array[VertexId],
              EdgeAttr: Array[Double],
              sourceList: ArrayBuffer[VertexId],
              pid :Int):
  Unit = {

    GPUShutdown(pid)
    var runningScript = ""

    if (envControl.controller == 0){
      runningScript =
        "/usr/local/ssspexample/cpp_native/build/bin/srv_UtilServerTest_BellmanFordGPU " + vertexSum.toString +
          " " + EdgeSrc.length.toString + " " + sourceList.length.toString + " " + pid.toString

    }
    else {
      runningScript =
        "./cpp_native/build/bin/srv_UtilServerTest_BellmanFordGPU " + vertexSum.toString +
          " " + EdgeSrc.length.toString + " " + sourceList.length.toString + " " + pid.toString

    }

    Process(runningScript).run()

    // too quickly for cuda init
    //Thread.sleep(1000)

    System.loadLibrary("PregelGPU")

    //initialize the source id array
    val sourceId = new util.ArrayList[Long](sourceList.length+(sourceList.length>>1))
    for(unit <- sourceList){
      sourceId.add(unit)
    }

    // if not success, it will run
    var result = false

    while(! result){
      result = GPUServerInit(filteredVertex, vertexSum, EdgeSrc, EdgeDst, EdgeAttr, sourceId, pid)
    }
  }

  // after executing, close the server and release the shared memory
  def GPUShutdown(pid: Int):Boolean = {

    System.loadLibrary("PregelGPU")
    val ifSuccess = GPUServerShutdown(pid)
    ifSuccess

  }
}
