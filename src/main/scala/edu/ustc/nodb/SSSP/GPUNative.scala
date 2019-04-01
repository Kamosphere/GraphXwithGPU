package edu.ustc.nodb.SSSP

import java.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexId

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

class GPUNative extends Serializable {

  // Define the vertex attribute in GPU-based SSSP project
  // Boolean stands for the activeness of Vertex
  // mutable.map stored the pairs of nearest distance from landmark
  type SPMapWithActive = (Boolean, mutable.Map[VertexId, Double])

  /* the GPU-based method to implement the SSSP */

  // native function
  // messageId is used to mark when the vertex will be in active in the next iteration
  // in the first iteration messageId is set in the graph (the boolean variable) when the graph created
  // after calculation the messageId will be overwritten in the native function
  // messageId and the returned hashmap will be mixed as EdgeTriplet
  @native def GPUSSSP(vertexNumber: Long,
                      VertexSets: util.ArrayList[VertexSet],
                      EdgeSets: util.ArrayList[EdgeSet],
                      sourceId: util.ArrayList[Long])
  : util.ArrayList[VertexSet]

  def GPUProcess(partitionVertex: util.ArrayList[VertexSet],
                 partitionEdge: util.ArrayList[EdgeSet],
                 allSource: Broadcast[List[VertexId]],
                 vertexNumbers: Long)
  : ArrayBuffer[(VertexId, SPMapWithActive)] = {

    //initialize the source id array
    val sourceId = new util.ArrayList[Long](allSource.value.length+(allSource.value.length>>1))
    for(unit <- allSource.value){
      sourceId.add(unit)
    }

    System.loadLibrary("GPUSSSP")

    //pass them through JNI
    val running = GPUSSSP(vertexNumbers, partitionVertex, partitionEdge, sourceId)

    // collect the results and carry them out
    val result = new ArrayBuffer[(VertexId, SPMapWithActive)]

    var temp : VertexSet = null
    val iter = running.iterator()
    while(iter.hasNext){
      temp = iter.next()
      result.+=((
        temp.VertexId(),
        (temp.ifActive(), temp.Attr().asScala
      )))
    }
    result
  }
}
