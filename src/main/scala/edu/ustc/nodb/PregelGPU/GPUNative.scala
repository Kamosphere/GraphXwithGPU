package edu.ustc.nodb.PregelGPU

import java.util

import edu.ustc.nodb.PregelGPU.Plugin.VertexSet
import org.apache.spark.graphx.VertexId

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.sys.process.Process

class GPUNative extends Serializable {

  // Define the vertex attribute in GPU-based SSSP project
  // Boolean stands for the activeness of Vertex
  // mutable.map stored the pairs of nearest distance from landmark
  type SPMapWithActive = (Boolean, mutable.LinkedHashMap[VertexId, Double])

  /* the GPU-based method to implement the SSSP */

  // native function to init the edge
  @native def GPUServerInit(filtedVertex: Array[Long],
                            vertexNumber: Long,
                            EdgeSrc: Array[VertexId],
                            EdgeDst: Array[VertexId],
                            EdgeAttr: Array[Double],
                            sourceId: util.ArrayList[Long],
                            pid:Int):
  Boolean

  // native function to execute SSSP algorithm in final step
  @native def GPUClientAllSSSP(vertexNumber: Long,
                            vertexSize:Int,
                            edgeSize: Int,
                            markIdSize: Int,
                            pid: Int,
                            resultID: Array[Long],
                            resultAttr: Array[Double]):
  Int

  // native function to execute SSSP algorithm while last iter skipped
  @native def GPUClientSkippedSSSP(vertexNumber: Long,
                            vertexSize:Int,
                            edgeSize: Int,
                            markIdSize: Int,
                            pid: Int,
                            resultID: Array[Long],
                            resultAttr: Array[Double]):
  Int

  // native function to execute SSSP algorithm
  @native def GPUClientSSSP(vertexNumber: Long,
                            VertexID: Array[Long],
                            VertexActive: Array[Boolean],
                            VertexAttr: Array[Double],
                            vertexSize:Int,
                            edgeSize: Int,
                            markIdSize: Int,
                            pid: Int,
                            resultID: Array[Long],
                            resultAttr: Array[Double]):
  Int

  // native function to close server
  @native def GPUServerShutdown(pid: Int):
  Boolean

  // execute SSSP algorithm
  def GPUAllProcess(vertexNumbers: Long,
                    vertexSize: Int,
                    edgeSize: Int,
                    sourceList: ArrayBuffer[VertexId],
                    pid: Int):
  ArrayBuffer[(VertexId, SPMapWithActive)] = {

    System.loadLibrary("GPUSSSP")

    val sourceSize = sourceList.length

    val resultID : Array[Long] = new Array[Long](vertexNumbers.toInt)
    val resultAttr : Array[Double] = new Array[Double](vertexNumbers.toInt * sourceSize)

    val results = new ArrayBuffer[(VertexId, SPMapWithActive)]

    var tempVertexSet : VertexSet = null

    //pass vertices through JNI and get arrayBuffer back
    val underIndex = GPUClientAllSSSP(vertexNumbers,
      vertexSize, edgeSize, sourceSize, pid,
      resultID, resultAttr)

    val startNew = System.nanoTime()

    var i = 0

    for(unit <- resultID){

      tempVertexSet = new VertexSet(unit, true)
      for(j <- sourceList.indices){
        tempVertexSet.addAttr(sourceList(j), resultAttr(i * sourceSize + j))
      }

      results.+=(tempVertexSet.TupleReturn())
      i = i + 1

    }
      val endNew = System.nanoTime()
      println("Constructing returned arrayBuffer time: " + (endNew - startNew))
      results
  }

  // execute SSSP algorithm
  def GPUSkippedProcess(
                 vertexNumbers: Long,
                 vertexSize: Int,
                 edgeSize: Int,
                 sourceList: ArrayBuffer[VertexId],
                 pid: Int):
  (ArrayBuffer[(VertexId, SPMapWithActive)], Boolean) = {

    System.loadLibrary("GPUSSSP")

    val sourceSize = sourceList.length

    val resultID : Array[Long] = new Array[Long](vertexNumbers.toInt)
    val resultAttr : Array[Double] = new Array[Double](vertexNumbers.toInt * sourceSize)

    val results = new ArrayBuffer[(VertexId, SPMapWithActive)]

    var tempVertexSet : VertexSet = null

    //pass vertices through JNI and get arrayBuffer back
    val underIndex = GPUClientSkippedSSSP(vertexNumbers,
      vertexSize, edgeSize, sourceSize, pid,
      resultID, resultAttr)

    val startNew = System.nanoTime()

    var i = 0

    for(unit <- resultID){

      tempVertexSet = new VertexSet(unit, true)
      for(j <- sourceList.indices){
        tempVertexSet.addAttr(sourceList(j), resultAttr(i * sourceSize + j))
      }

      results.+=(tempVertexSet.TupleReturn())
      i = i + 1

    }

    if(underIndex < 0){

      val endNew = System.nanoTime()

      println("Constructing returned null arrayBuffer time: " + (endNew - startNew))

      (results, false)
    }
    else{

      val endNew = System.nanoTime()

      println("Constructing returned arrayBuffer time: " + (endNew - startNew))

      (results, true)
    }

  }
  // execute SSSP algorithm
  def GPUProcess(VertexID: Array[Long],
                 VertexActive: Array[Boolean],
                 VertexAttr: Array[Double],
                 vertexNumbers: Long,
                 vertexSize: Int,
                 edgeSize: Int,
                 sourceList: ArrayBuffer[VertexId],
                 pid: Int):
  (ArrayBuffer[(VertexId, SPMapWithActive)], Boolean) = {

    System.loadLibrary("GPUSSSP")

    val sourceSize = sourceList.length

    val resultID : Array[Long] = new Array[Long](vertexNumbers.toInt)
    val resultAttr : Array[Double] = new Array[Double](vertexNumbers.toInt * sourceSize)

    val results = new ArrayBuffer[(VertexId, SPMapWithActive)]

    var tempVertexSet : VertexSet = null

    // pass vertices through JNI and get result array back
    val underIndex = GPUClientSSSP(vertexNumbers,
      VertexID, VertexActive, VertexAttr,
      vertexSize, edgeSize, sourceSize, pid,
      resultID, resultAttr)

    val startNew = System.nanoTime()

    // take results out through VertexSet
    var i = 0

    for(unit <- resultID){

      tempVertexSet = new VertexSet(unit, true)
      for(j <- sourceList.indices){
        tempVertexSet.addAttr(sourceList(j), resultAttr(i * sourceSize + j))
      }

      results.+=(tempVertexSet.TupleReturn())
      i = i + 1

    }

    if(underIndex < 0){

      val endNew = System.nanoTime()

      println("Constructing returned null arrayBuffer time: " + (endNew - startNew))

      (results, false)
    }
    else{

      val endNew = System.nanoTime()

      println("Constructing returned arrayBuffer time: " + (endNew - startNew))

      (results, true)
    }

  }

  // before executing, run the server first
  def GPUInit(vertexCount: Long,
              filteredVertex: Array[Long],
              EdgeSrc: Array[VertexId],
              EdgeDst: Array[VertexId],
              EdgeAttr: Array[Double],
              sourceList: ArrayBuffer[VertexId],
              pid :Int):
  Boolean = {

    GPUShutdown(pid)

    val runningScript =
      "./cpp_native/build/bin/srv_UtilServerTest_BellmanFordGPU " + vertexCount.toString +
        " " + EdgeSrc.length.toString + " " + sourceList.length.toString + " " + pid.toString

    Process(runningScript).run()

    // too quickly for cuda init
    Thread.sleep(500)

    System.loadLibrary("GPUSSSP")

    //initialize the source id array
    val sourceId = new util.ArrayList[Long](sourceList.length+(sourceList.length>>1))
    for(unit <- sourceList){
      sourceId.add(unit)
    }

    // if not success, it will run again outside
    val result = GPUServerInit(filteredVertex, vertexCount, EdgeSrc, EdgeDst, EdgeAttr, sourceId, pid)

    result

  }

  // after executing, close the server and release the shared memory
  def GPUShutdown(pid: Int):Boolean = {

    System.loadLibrary("GPUSSSP")
    val ifSuccess = GPUServerShutdown(pid)
    ifSuccess

  }
}
