package org.apache.spark.graphx.impl

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class modifiedGraphImpl[VD: ClassTag, ED: ClassTag] protected (
                                                        @transient override val vertices: VertexRDD[VD],
                                                        @transient override val replicatedVertexView: ReplicatedVertexView[VD, ED])
  extends GraphImpl[VD, ED](vertices, replicatedVertexView) with Serializable {

  def this(g: Graph[VD, ED]) = this(g.vertices, new ReplicatedVertexView(g.edges.asInstanceOf[EdgeRDDImpl[ED, VD]], true, true))

  def messageUpdateExtract(activeSetOpt: Option[(VertexRDD[VD], EdgeDirection)]): RDD[EdgeTriplet[VD, ED]] = {

    vertices.cache()
    // For each vertex, replicate its attribute only to partitions where it is
    // in the relevant position in an edge.
    replicatedVertexView.upgrade(vertices, includeSrc = true, includeDst = true)
    val view = activeSetOpt match {
      case Some((activeSet, _)) =>
        replicatedVertexView.withActiveSet(activeSet)
      case None =>
        replicatedVertexView
    }

    val triplet = view.edges.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, part) => part.tripletIterator()
    })

    triplet
  }

}
