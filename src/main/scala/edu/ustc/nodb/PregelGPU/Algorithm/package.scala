package edu.ustc.nodb.PregelGPU

import org.apache.spark.graphx.VertexId

import scala.collection.mutable

package object Algorithm {

  // Define the vertex attribute in GPU-based SSSP project
  // Boolean stands for the activeness of Vertex
  // mutable.LinkedHashMap stored the pairs of nearest distance from landmark in order
  type SPMapWithActive = (Boolean, mutable.LinkedHashMap[VertexId, Double])

}
