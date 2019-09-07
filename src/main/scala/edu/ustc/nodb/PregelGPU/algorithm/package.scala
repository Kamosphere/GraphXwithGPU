package edu.ustc.nodb.PregelGPU

import org.apache.spark.graphx.VertexId

import scala.collection.mutable

package object algorithm {

  // Define the vertex attribute in GPU-based SSSP project
  // mutable.LinkedHashMap stored the pairs of nearest distance from landmark in order
  type SPMapWithActive = mutable.LinkedHashMap[VertexId, Double]

}
