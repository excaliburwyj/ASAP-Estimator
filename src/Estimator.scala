import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, Graph}

import scala.collection
import scala.collection.mutable.Map
import scala.collection.mutable.Set
import scala.util.Random

abstract class Estimator[VD,ED](wholeGraph: Graph[VD,ED]){
  var estimationValue = 0.0
  //这一步是把GraphX中的边集转成Array，不知道是不是拿到内存中
  // 如果是的话，特别大的图可能会消耗内存很大，实验时如果带来性能瓶颈可能需要修改
  val edges = wholeGraph.edges.collect()
  var streamIndex = 0
  var sampleEdgeList = List[Edge[ED]]()
  var iteratorEdge = edges.iterator

  var neighborEdges = Array[Edge[ED]]()

  def algorithm() : Int

  def SampleEdge(): (Edge[ED], Double) = {
    //TODO uniform sample from edges
    val count = edges.size
    streamIndex = Random.nextInt(count)
    iteratorEdge = iteratorEdge.drop(streamIndex+1)
    if (0 == wholeGraph.edges.count()) {
      return (null, 0)
    }
    (edges(streamIndex),1.0/wholeGraph.edges.count())
  }
  def ConditionalSampleEdge(subEdges:List[Edge[ED]]): (Edge[ED],Double)={
    //TODO uniform sample from e 's neighborhood
    var vertexSet : Set[graphx.VertexId]= Set()
    subEdges.foreach(edge=>{
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
    (neighborE, 1.0/neighborCount)
  }
  def ConditionalClose(subEdges:List[Edge[ED]], expectEdges:List[Edge[ED]]) : Boolean={
    var findExpect : collection.mutable.Map[Edge[ED], Boolean] = collection.mutable.Map( expectEdges.map(e=>(e,false)):_*)
    edges.drop(streamIndex+1).foreach(edge =>{
       if(findExpect.contains(edge)){
         findExpect(edge) = true
       }
    })
    findExpect.count(it=>it._2) == findExpect.size

  }
}

class TriangleEstimator[VD,ED](wholeGraph: Graph[VD,ED]) extends Estimator[VD,ED](wholeGraph) {

//  override def ConditionalClose(subEdges: List[Edge[ED]], expectEdges: List[Edge[ED]]): Boolean = {
//    var vertex: Map[graphx.VertexId, Int] = Map[graphx.VertexId, Int](1.toLong -> 0, 2.toLong -> 0, 3.toLong -> 0, 4.toLong -> 0, 5.toLong -> 0)
//    for (e <- subEdges) {
//      vertex += (e.srcId.toLong -> (vertex(e.srcId) + 1).toInt)
//      vertex += (e.dstId.toLong -> (vertex(e.dstId) + 1).toInt)
//    }
//    var expectVertex = vertex.filter(it => it._2 == 1)
//    if (expectVertex.size < 2) {
//      return false
//    }
//    else {
//      var expectArray = expectVertex.take(2).toArray
//      var flag = false
//      for (item <- neighborEdges) {
//        if ((item.dstId == expectArray(0)._1 & item.srcId == expectArray(1)._1)
//          | (item.dstId == expectArray(1)._1 & item.srcId == expectArray(0)._1)) {
//          flag = true
//        }
//      }
//      if (true == flag)
//        return true
//      else
//        return false
//    }
//  }

  override def algorithm(): Int = {


    val spl = SampleEdge()
    if(0==spl._2){
      return 0
    }
    sampleEdgeList = sampleEdgeList.+:(spl._1)
    val cdtSpl = ConditionalSampleEdge(sampleEdgeList)
    if(0==cdtSpl._2){
      return 0
    }
    sampleEdgeList = sampleEdgeList.+:(cdtSpl._1)

    var vertex: Map[graphx.VertexId, Int] = Map[graphx.VertexId, Int]()
    sampleEdgeList.foreach(e => {
      if (vertex.contains(e.srcId)) {
        vertex(e.srcId) += 1
      }
      else vertex += (e.srcId -> 1)
      if (vertex.contains(e.dstId)) {
        vertex(e.dstId) += 1
      }
      else vertex += (e.dstId -> 1)

    })
    var expectVertex = vertex.filter(it => it._2 == 1)
    if (expectVertex.size < 2) {
      0
    }
    else {
      var expectArray = expectVertex.take(2).toArray
      val expectEdge1 = new Edge[ED](expectArray(0)._1, expectArray(1)._1)
      val expectEdge2 = new Edge[ED](expectArray(1)._1, expectArray(0)._1)

      if (!ConditionalClose(sampleEdgeList, List(expectEdge1)) && !ConditionalClose(sampleEdgeList, List(expectEdge2))) {
        0
      }
      else {
        (1 / spl._2 * cdtSpl._2).toInt
      }
    }
  }
}