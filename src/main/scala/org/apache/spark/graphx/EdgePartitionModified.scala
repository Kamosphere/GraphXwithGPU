package org.apache.spark.graphx

import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

import scala.reflect.ClassTag

class EdgePartitionModified[@specialized(Char, Int, Boolean, Byte, Long, Float, Double)
ED: ClassTag, VD: ClassTag](localSrcIds: Array[Int],
                            localDstIds: Array[Int],
                            data: Array[ED],
                            index: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
                            global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
                            local2global: Array[VertexId],
                            vertexAttrs: Array[VD],
                            activeSet: Option[VertexSet])
  extends EdgePartition[ED, VD](localSrcIds, localDstIds, data, index, global2local, local2global, vertexAttrs, activeSet) with Serializable {

  def vertexDstIterator()
  : Iterator[(VertexId, VD)] = new Iterator[(VertexId, VD)] {
    private[this] var pos = 0

    override def hasNext: Boolean = pos < this.size

    override def next(): (VertexId, VD) = {
      val localDstId = localDstIds(pos)
      val dstAttr = vertexAttrs(localDstId)
      val dstId = local2global(localDstId)
      pos += 1
      (dstId, dstAttr)
    }
  }
}
