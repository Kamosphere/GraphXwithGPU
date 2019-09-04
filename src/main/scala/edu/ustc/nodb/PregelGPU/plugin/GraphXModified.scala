package edu.ustc.nodb.PregelGPU.plugin

import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.modifiedGraphImpl
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object GraphXModified {

  // advanced version of joinVertices
  // for data in graph : do mapFunc or defaultFunc
  def joinVerticesOrModify[U: ClassTag, VD: ClassTag, ED: ClassTag]
  (graph: Graph[VD, ED],
   table: RDD[(VertexId, U)])
  (mapFunc: (VertexId, VD, U) => VD)
  (defaultFunc: VD => VD):
  Graph[VD, ED] = {

    val uf = (id: VertexId, data: VD, o: Option[U]) => {
      o match {
        case Some(u) => mapFunc(id, data, u)
        case None => defaultFunc(data)
      }
    }
    graph.outerJoinVertices(table)(uf)
  }

  // Use the graphX way of merging message
  def msgExtract[VD: ClassTag, ED: ClassTag]
  (graph: Graph[VD, ED],
   activeSetOpt: Option[(VertexRDD[VD], EdgeDirection)]):
  RDD[EdgeTriplet[VD, ED]] = {

    val newOne = new modifiedGraphImpl(graph)
    newOne.messageUpdateExtract(activeSetOpt)
  }
}
