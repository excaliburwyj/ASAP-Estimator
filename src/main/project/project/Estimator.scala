package project

import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, Graph}

import scala.collection.mutable.Set
import scala.util.Random

abstract class Estimator[VD, ED](wholeGraph: Graph[VD, ED]) {
  var estimationValue = 0.0
  val edges = wholeGraph.edges.collect()
  var streamIndex = 0
  var sampleEdgeList = List[Edge[ED]]()
  var iteratorEdge = edges.iterator
  var neighborEdges = Array[Edge[ED]]()

  def algorithm(): Int

  def SampleEdge(): (Edge[ED], Double) = {
    //TODO uniform sample from edges
    val count = edges.size
    streamIndex = Random.nextInt(count)
    iteratorEdge = iteratorEdge.drop(streamIndex+1)
    if (0 == wholeGraph.edges.count()) {
      return (null, 0)
    }
    (edges(streamIndex), 1.0 / wholeGraph.edges.count())
  }

  def ConditionalSampleEdge(subEdges: List[Edge[ED]]): (Edge[ED], Double) = {
    //TODO uniform sample from e 's neighborhood
    var vertexSet: Set[graphx.VertexId] = Set()
    subEdges.foreach(edge => {
      vertexSet.add(edge.srcId)
      vertexSet.add(edge.dstId)
    })
    neighborEdges = edges.drop(streamIndex+1).filter(edge => {
      vertexSet.contains(edge.srcId) || vertexSet.contains(edge.dstId)
    })
    val neighborCount = neighborEdges.size
    if (0 == neighborCount) {
      return (null, 0)
    }
    var neighborIndex = Random.nextInt(neighborCount)
    val neighborE = neighborEdges(neighborIndex)
    streamIndex = edges.indexOf(neighborE)
    (neighborE, 1.0 / neighborCount)
  }

  def ConditionalClose(subEdges: List[Edge[ED]], expectEdges: List[Edge[ED]]): Boolean = {
    //TODO uniform sample from e 's neighborhood
    true
  }
}
