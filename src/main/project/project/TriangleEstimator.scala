package project

import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, Graph}

import scala.collection.mutable.Map


class TriangleEstimator[VD,ED](wholeGraph: Graph[VD,ED]) extends Estimator[VD,ED](wholeGraph){
  //specified to count triangles, to be generalized
  override def ConditionalClose(subEdges: List[Edge[ED]], expectEdges: List[Edge[ED]]): Boolean = {
    var vertex: Map[graphx.VertexId,Int] = Map[graphx.VertexId,Int](1.toLong->0,2.toLong->0,3.toLong->0,4.toLong->0,5.toLong->0)
    for(e <- subEdges){
      vertex += (e.srcId.toLong -> (vertex(e.srcId) + 1).toInt)
      vertex += (e.dstId.toLong -> (vertex(e.dstId) + 1).toInt)
    }
    var expectVertex = vertex.filter(it=>it._2==1)
    if(expectVertex.size < 2){
      return false
    }
    else{
      var expectArray = expectVertex.take(2).toArray
      var flag=false
      for(item <- neighborEdges){
        if((item.dstId==expectArray(0)._1 & item.srcId==expectArray(1)._1) | (item.dstId==expectArray(1)._1 & item.srcId==expectArray(0)._1)){
          flag=true
        }
      }
      if(true==flag)
        return true
      else
        return false
    }
  }

  override def algorithm(): Int = {
    estimationValue = 0.0
    streamIndex = 0
    sampleEdgeList = List[Edge[ED]]()
    iteratorEdge = edges.iterator
    neighborEdges = Array[Edge[ED]]()

    val spl = SampleEdge()
    if(0==spl._2){
      return 0
    }
    sampleEdgeList = sampleEdgeList.+:(spl._1)
    val cdtSpl = ConditionalSampleEdge(sampleEdgeList)
    if(0==cdtSpl._2){
      return 0
    }
    //TODO Triangle left edge
    sampleEdgeList = sampleEdgeList.+:(cdtSpl._1)
    if(ConditionalClose(sampleEdgeList, null)){
      return (1.0/(spl._2*cdtSpl._2)).toInt
    }else{
      return 0
    }
  }
}