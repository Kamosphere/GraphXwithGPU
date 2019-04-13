package edu.ustc.nodb.SSSP

import java.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexId

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.sys.process.Process

class GPUNative extends Serializable {

  // Define the vertex attribute in GPU-based SSSP project
  // Boolean stands for the activeness of Vertex
  // mutable.map stored the pairs of nearest distance from landmark
  type SPMapWithActive = (Boolean, mutable.Map[VertexId, Double])

  /* the GPU-based method to implement the SSSP */

  // native function to execute SSSP algorithm
  @native def GPUClientSSSP(vertexNumber: Long,
                            VertexSets: util.ArrayList[VertexSet],
                            edgeSize: Int,
                            markIdSize: Int,
                            pid:Int)
  : ArrayBuffer[(VertexId, SPMapWithActive)]

  // native function to close server
  @native def GPUServerShutdown(pid: Int)
  : Boolean

  // native function to init the edge
  @native def GPUServerInit(vertexNumber: Long,
                            EdgeSets: util.ArrayList[EdgeSet],
                            sourceId: util.ArrayList[Long],
                            pid:Int)
  : Boolean

  // execute SSSP algorithm
  def GPUProcess(partitionVertex: util.ArrayList[VertexSet],
                 vertexNumbers: Long,
                 edgeSize: Int,
                 sourceAmount: Int,
                 pid: Int)
  : ArrayBuffer[(VertexId, SPMapWithActive)] = {

    System.loadLibrary("GPUSSSP")

    //pass vertices through JNI and get arrayBuffer back
    val result = GPUClientSSSP(vertexNumbers, partitionVertex, edgeSize, sourceAmount, pid)

    result
  }

  // before executing, run the server first
  def GPUInit(vertexCount: Long, Edge: util.ArrayList[EdgeSet], sourceList: Array[VertexId], pid :Int):Boolean = {

    val runningScript =
      "./cpp_native/build/bin/srv_UtilServerTest_BellmanFordGPU " + vertexCount.toString +
        " " + Edge.size().toString + " " + sourceList.length.toString + " " + pid.toString

    Process(runningScript).run()

    // too quickly for cuda init
    Thread.sleep(2000)

    System.loadLibrary("GPUSSSP")

    //initialize the source id array
    val sourceDistinctOrdered = ArrayBuffer(sourceList:_*).distinct.sorted
    val sourceId = new util.ArrayList[Long](sourceList.length+(sourceList.length>>1))
    for(unit <- sourceDistinctOrdered){
      sourceId.add(unit)
    }

    // if not success, it will run again outside
    val result = GPUServerInit(vertexCount, Edge, sourceId, pid)

    result

  }

  // after executing, close the server and release the shared memory
  def GPUShutdown(pid: Int):Boolean = {

    System.loadLibrary("GPUSSSP")
    val ifSuccess = GPUServerShutdown(pid)
    ifSuccess

  }
}
