package edu.ustc.nodb.GPUGraphX.example.PageRank

import org.apache.spark.graphx._

import scala.reflect.ClassTag

class PregelSparkPageRank extends Serializable {

  def runUntilConvergenceWithOptions[VD: ClassTag, ED: ClassTag]
  (graph: Graph[VD, ED], tol: Double, resetProb: Double,
   srcId: Option[VertexId] = None): Graph[Double, Double] =
  {
    require(tol >= 0, s"Tolerance must be no less than 0, but got $tol")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got $resetProb")

    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)

    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 0.
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to (initialPR, delta = 0)
      .mapVertices { (id, attr) =>
      if (id == src) (0.0, Double.NegativeInfinity) else (0.0, 0.0)
    }.persist()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    def personalizedVertexProgram(id: VertexId, attr: (Double, Double),
                                  msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = if (lastDelta == Double.NegativeInfinity) {
        1.0
      } else {
        oldPR + (1.0 - resetProb) * msgSum
      }
      (newPR, newPR - oldPR)
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = if (personalized) 0.0 else resetProb / (1.0 - resetProb)

    // Execute a dynamic version of Pregel.
    val vp = if (personalized) {
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        personalizedVertexProgram(id, attr, msgSum)
    } else {
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        vertexProgram(id, attr, msgSum)
    }

    val rankGraph = Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vp, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)

    // SPARK-18847 If the graph has sinks (vertices with no outgoing edges) correct the sum of ranks
    normalizeRankSum(rankGraph, personalized)
  }

  // Normalizes the sum of ranks to n (or 1 if personalized)
  private def normalizeRankSum(rankGraph: Graph[Double, Double], personalized: Boolean) = {
    val rankSum = rankGraph.vertices.values.sum()
    if (personalized) {
      rankGraph.mapVertices((id, rank) => rank / rankSum)
    } else {
      val numVertices = rankGraph.numVertices
      val correctionFactor = numVertices.toDouble / rankSum
      rankGraph.mapVertices((id, rank) => rank * correctionFactor)
    }
  }
}
