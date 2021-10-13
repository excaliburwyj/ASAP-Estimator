import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, Graph}

import scala.collection.mutable.Map
import scala.collection.mutable.Set
import scala.util.Random

abstract class Estimator[VD,ED](wholeGraph: Graph[VD,ED]){
  var estimationValue = 0.0
  val edges = wholeGraph.edges.collect()
  var streamIndex = 0
  var sampleEdgeList = List[Edge[ED]]()
  var iteratorEdge = edges.iterator
  def algorithm() : Int

  def SampleEdge(): (Edge[ED], Double) = {
    //TODO uniform sample from edges
    val count = edges.size
    streamIndex = Random.nextInt(count)
    iteratorEdge = iteratorEdge.drop(streamIndex)
    (edges(streamIndex),1.0/wholeGraph.edges.count())
  }
  def ConditionalSampleEdge(subEdges:List[Edge[ED]]): (Edge[ED],Double)={
    //TODO uniform sample from e 's neighborhood
    var vertexSet : Set[graphx.VertexId]= Set()
    subEdges.foreach(edge=>{
      vertexSet.add(edge.srcId)
      vertexSet.add(edge.dstId)
    })
    val neighborEdges = edges.drop(streamIndex).filter(edge=>{
      vertexSet.contains(edge.srcId) || vertexSet.contains(edge.dstId)
    })
    val neighborCount = neighborEdges.size
    val neighborIndex = Random.nextInt(neighborCount)
    val neighborE = neighborEdges(neighborIndex)
    streamIndex = edges.indexOf(neighborE)
    (neighborE, 1.0/neighborCount)
  }
  def ConditionalClose(subEdges:List[Edge[ED]], expectEdges:List[Edge[ED]]) : Boolean={
    //TODO uniform sample from e 's neighborhood
    true
  }
}

class TriangleEstimator[VD,ED](wholeGraph: Graph[VD,ED]) extends Estimator[VD,ED](wholeGraph){
  override def algorithm(): Int = {

    val spl = SampleEdge()
    sampleEdgeList.+:(spl._1)
    val cdtSpl = ConditionalSampleEdge(sampleEdgeList)
    sampleEdgeList.+:(cdtSpl._1)
    //TODO Triangle left edge
    var vertex: Map[graphx.VertexId,Int] = Map[graphx.VertexId,Int]()
    sampleEdgeList.foreach(e=>{
      if(vertex.contains(e.srcId)){
        vertex(e.srcId) +=1
      }
      else vertex += (e.srcId->0)
      if(vertex.contains(e.dstId)){
        vertex(e.dstId) +=1
      }
      else vertex += (e.dstId->0)

    })
    var expectVertex = vertex.filter(it=>it._2==1)
    if(expectVertex.size < 2){
      0
    }
    else{
      var expectArray = expectVertex.take(2).toArray
      val expectEdge = new Edge[ED](expectArray(0)._1,expectArray(1)._1)
      if(ConditionalClose(sampleEdgeList, List(expectEdge))){
        0
      }
      else{
        (1/spl._2*cdtSpl._2).toInt
      }
    }
  }
}