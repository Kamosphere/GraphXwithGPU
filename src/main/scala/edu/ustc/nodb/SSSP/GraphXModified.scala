package edu.ustc.nodb.SSSP

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object GraphXModified {

  // if find data then run mapFunc.if not, then run defaultFunc
  def joinVerticesDefault[U: ClassTag, VD:ClassTag, ED:ClassTag]
  (graph: Graph[VD, ED], table: RDD[(VertexId, U)])
  (mapFunc: (VertexId, VD, U) => VD)(defaultFunc: VD => VD)
  : Graph[VD, ED] = {
    val uf = (id: VertexId, data: VD, o: Option[U]) => {
      o match {
        case Some(u) => mapFunc(id, data, u)
        case None => defaultFunc(data)
      }
    }
    graph.outerJoinVertices(table)(uf)
  }
}
