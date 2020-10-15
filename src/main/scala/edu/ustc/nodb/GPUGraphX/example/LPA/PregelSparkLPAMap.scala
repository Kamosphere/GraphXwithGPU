package edu.ustc.nodb.GPUGraphX.example.LPA

import org.apache.spark.graphx._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class PregelSparkLPAMap [VD, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

  def sendMessage(e: EdgeTriplet[(VertexId, Long, ArrayBuffer[(Int, Long)]), ED]):
  Iterator[(VertexId, Map[VertexId, Long])] = {

    Iterator((e.dstId, Map(e.srcAttr._1 -> 1L)))
  }

  def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
  : Map[VertexId, Long] = {
    (count1.keySet ++ count2.keySet).map { i =>
      val count1Val = count1.getOrElse(i, 0L)
      val count2Val = count2.getOrElse(i, 0L)
      i -> (count1Val + count2Val)
    }(collection.breakOut) // more efficient alternative to [[collection.Traversable.toMap]]
  }

  def vertexProgram(vid: VertexId, attr: (VertexId, Long, ArrayBuffer[(Int, Long)]), message: Map[VertexId, Long]):
  (VertexId, Long, ArrayBuffer[(Int, Long)]) = {

    if (message.isEmpty){
      val tail = (attr._3.last._1 + 1, 0L)
      attr._3.+=(tail)
      attr

    } else{
      val pair = message.maxBy(_._2)
      val tail = (attr._3.last._1 + 1, message.size.toLong)
      attr._3.+=(tail)
      (pair._1, pair._2, attr._3)
    }
  }

  val initialMessage: Map[VertexId, VertexId] = Map[VertexId, Long]()

  def run(maxSteps: Int): Graph[(VertexId, Long, ArrayBuffer[(Int, Long)]), ED] = {

    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got $maxSteps")

    val lpaGraph = graph.mapVertices { case (vid, _) =>
      val arr = new ArrayBuffer[(Int, Long)]()
      arr.+=((0,0L))
      (vid, 0L, arr)
    }

    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps, EdgeDirection.Out)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }

  def runInContinue(prevGraph: Graph[(VertexId, Long, ArrayBuffer[(Int, Long)]), ED], maxSteps: Int): Graph[(VertexId, Long, ArrayBuffer[(Int, Long)]), ED] = {

    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got $maxSteps")

    val lpaGraph = prevGraph

    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps, EdgeDirection.Out)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }
}
