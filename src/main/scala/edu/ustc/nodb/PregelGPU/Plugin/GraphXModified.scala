package edu.ustc.nodb.PregelGPU.Plugin

import edu.ustc.nodb.PregelGPU.Algorithm.lambdaTemplete
import org.apache.spark.graphx.impl.modifiedGraphImpl
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object GraphXModified {

  def joinVerticesOrModify[U: ClassTag, VD:ClassTag, ED:ClassTag](graph: Graph[VD, ED],
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

  def msgExtract[VD:ClassTag, ED:ClassTag](graph: Graph[VD, ED],
                                           activeSetOpt: Option[(VertexRDD[VD], EdgeDirection)]):
  RDD[EdgeTriplet[VD, ED]] = {

    val newOne = new modifiedGraphImpl(graph)
    newOne.messageUpdateExtract(activeSetOpt)
  }
}
