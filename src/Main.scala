import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.jfree.chart.ChartFrame

import scala.collection.mutable._
import org.jfree.chart._
import org.jfree.chart.ChartFactory
import org.jfree.chart.JFreeChart
import org.jfree.chart.axis.CategoryLabelPositions
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.data.xy.DefaultXYDataset

import java.util.concurrent.CountDownLatch

case class User(name: String, age: Int, inDeg: Int, outDeg: Int) extends Serializable

object GraphXExample {

  var lessOne = new ArrayBuffer[(Int, Int)]

  def main(args: Array[String]) {

    //屏蔽日志

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)



    //设置运行环境

    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")

    val sc = new SparkContext(conf)



    //设置顶点和边，注意顶点和边都是用元组定义的Array

    //顶点的数据类型是VD:(String,Int)

    val vertexArray = Array(

      (1L, ("Alice", 28)),

      (2L, ("Bob", 27)),

      (3L, ("Charlie", 65)),

      (4L, ("David", 42)),

      (5L, ("Ed", 55)),

      (6L, ("Fran", 50)),

      (7L, ("Wong", 24))

    )

    //边的数据类型ED:Int

    val edgeArray = Array(

      Edge(2L, 1L, 7),

      Edge(2L, 4L, 2),

      Edge(3L, 2L, 4),

      Edge(3L, 6L, 3),

      Edge(4L, 1L, 1),

      Edge(5L, 2L, 2),

      Edge(5L, 3L, 8),

      Edge(5L, 6L, 3),

      Edge(7L, 6L, 5),

      Edge(7L, 4L, 1),

      Edge(7L, 2L, 3)

    )



    //构造vertexRDD和edgeRDD

    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)

    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)



    //构造图Graph[VD,ED]

    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    val estimatorCount = 50
    var sumCount = new Array[Int](_length=estimatorCount)
    val countDownLatch = new CountDownLatch(estimatorCount)
    countDownLatch.countDown()
    for(i <- 0 until estimatorCount) {
      val thread = new Thread(() => {
        val test: TriangleEstimator[(String, Int), Int] = new TriangleEstimator[(String, Int), Int](graph)
        val count = test.algorithm()
//        print(s"${i} count ${count};  ")
        sumCount(i) = count
        countDownLatch.countDown()
      })
      thread.start()
    }
    countDownLatch.await()
    println()
    println(s"estimator ${estimatorCount} ${sumCount.sum/estimatorCount}")
    val triCounts = graph.triangleCount().vertices

    var totalTri = triCounts.reduce((a,b)=>{ (-1,a._2+b._2)})
    println(s"True Triangle ${totalTri._2 / 3}")

    //    val fbTri = facebook.triangleCount().vertices
    //
    //    var totalTri = fbTri.reduce((a,b)=>{ (-1,a._2+b._2)})
    ////    fbTri.foreach(
    ////      (it) => {
    ////        totalTri += it._2
    ////        println(it._2)
    ////      }
    ////    )
    //    println(s"FaceBook Triangle ${totalTri._2}")
//
//
//
//    //***********************************************************************************
//
//    //***************************  图的属性    ****************************************
//
//    //**********************************************************************************         println("***********************************************")
//
//    println("属性演示")
//
//    println("**********************************************************")
//
//    println("找出图中年龄大于30的顶点：")
//
//    graph.vertices.filter { case (id, (name, age)) => age > 30}.collect.foreach {
//
//      case (id, (name, age)) => println(s"$name is $age")
//
//    }
//
//
//
//    //边操作：找出图中属性大于5的边
//
//    println("找出图中属性大于5的边：")
//
//    graph.edges.filter(e => e.attr > 5).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
//
//    println
//
//
//
//    //triplets操作，((srcId, srcAttr), (dstId, dstAttr), attr)
//
//    println("列出边属性>5的tripltes：")
//
//    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
//
//      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
//
//    }
//
//    println
//
//
//
//    //Degrees操作
//
//    println("找出图中最大的出度、入度、度数：")
//
//    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
//
//      if (a._2 > b._2) a else b
//
//    }
//
//    println("max of outDegrees:" + graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" + graph.degrees.reduce(max))
//
//    println
//
//
//
//    //***********************************************************************************
//
//    //***************************  转换操作    ****************************************
//
//    //**********************************************************************************
//
//    println("**********************************************************")
//
//    println("转换操作")
//
//    println("**********************************************************")
//
//    println("顶点的转换操作，顶点age + 10：")
//
//    graph.mapVertices{ case (id, (name, age)) => (id, (name, age+10))}.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
//
//    println
//
//    println("边的转换操作，边的属性*2：")
//
//    graph.mapEdges(e=>e.attr*2).edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
//
//    println
//
//
//
//    //***********************************************************************************
//
//    //***************************  结构操作    ****************************************
//
//    //**********************************************************************************
//
//    println("**********************************************************")
//
//    println("结构操作")
//
//    println("**********************************************************")
//
//    println("顶点年纪>30的子图：")
//
//    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)
//
//    println("子图所有顶点：")
//
//    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
//
//    println
//
//    println("子图所有边：")
//
//    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
//
//    println
//
//
//
//
//
//    //***********************************************************************************
//
//    //***************************  连接操作    ****************************************
//
//    //**********************************************************************************
//
//    println("**********************************************************")
//
//    println("连接操作")
//
//    println("**********************************************************")
//
//    val inDegrees: VertexRDD[Int] = graph.inDegrees
//
//
//
//
//    //创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
//
//    val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0)}
//
//
//
//    //initialUserGraph与inDegrees、outDegrees（RDD）进行连接，并修改initialUserGraph中inDeg值、outDeg值
//
//    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
//
//      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
//
//    }.outerJoinVertices(initialUserGraph.outDegrees) {
//
//      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg,outDegOpt.getOrElse(0))
//
//    }
//
//
//
//    println("连接图的属性：")
//
//    userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg}"))
//
//    println
//
//
//
//    println("出度和入读相同的人员：")
//
//    userGraph.vertices.filter {
//
//      case (id, u) => u.inDeg == u.outDeg
//
//    }.collect.foreach {
//
//      case (id, property) => println(property.name)
//
//    }
//
//    println
//
//
//
//    //***********************************************************************************
//
//    //***************************  聚合操作    ****************************************
//
//    //**********************************************************************************
//
//    println("**********************************************************")
//
//    println("聚合操作")
//
//    println("**********************************************************")
//
//    println("找出年纪最大的追求者：")
//
//    val oldestFollower: VertexRDD[(String, Int)] =userGraph.aggregateMessages[(String,Int)](
//            // 将源顶点的属性发送给目标顶点，map过程
//
//            edge => {
////              val it = Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age)))
////              while (it.hasNext){
////                println(it.next()._1)
////              }
//              edge.w((edge.srcAttr.name, edge.srcAttr.age))
//            },
//
//            // 得到最大追求者，reduce过程
//
//            (a, b) => if (a._2 > b._2) a else b
//    )
//    oldestFollower.foreach{
//      case (id, property) => println(property._1)
//    }
//
//    userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
//
//      optOldestFollower match {
//
//        case None => s"${user.name} does not have any followers."
//
//        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
//
//      }
//
//    }.collect.foreach { case (id, str) => println(str)}
//
//    println
//
//
//
//    //***********************************************************************************
//
//    //***************************  实用操作    ****************************************
//
//    //**********************************************************************************
//
//    println("**********************************************************")
//
//    println("聚合操作")
//
//    println("**********************************************************")
//
//    println("找出5到各顶点的最短：")
//
//    val sourceId: VertexId = 5L // 定义源点
//
//    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
//
//    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
//
//      (id, dist, newDist) => math.min(dist, newDist),
//
//      triplet => {  // 计算权重
//
//        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
//
//          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
//
//        } else {
//          Iterator.empty
//        }
//
//      },
//
//      (a,b) => math.min(a,b) // 最短距离
//
//    )
//
//    println(sssp.vertices.collect.mkString("\n"))
//
//    //***********************************************************************************
//
//    //***************************  找三角    ****************************************
//
//    //**********************************************************************************
//    println("**********************************************************")
//
//    println("寻找三角形")
//
//    val triCounts = graph.triangleCount().vertices
//
//
//    triCounts.foreach(
//      (it) => {println(s"${it._1} count: ${it._2}")}
//    )

//    //***********************************************************************************
//
//    //***************************  读取FaceBook图    ****************************************
//
//    //**********************************************************************************
//
//    println("**********************************************************")
//
//    println("读取FaceBook图")
//
//    val facebook  = GraphLoader.edgeListFile(sc,
//      "E:\\graph\\datagraph\\facebook_combined.txt").partitionBy(PartitionStrategy.RandomVertexCut)
//
////    val facebook = GraphLoader.edgeListFile(sc,
////      "E:\\graph\\spark-3.1.2-bin-hadoop3.2\\data\\graphx\\followers.txt").partitionBy(PartitionStrategy.RandomVertexCut)
////    println("FaceBook三角形")
//
//    val fbTri = facebook.triangleCount().vertices
//
//    var totalTri = fbTri.reduce((a,b)=>{ (-1,a._2+b._2)})
////    fbTri.foreach(
////      (it) => {
////        totalTri += it._2
////        println(it._2)
////      }
////    )
//    println(s"FaceBook Triangle ${totalTri._2}")
//
//    println("**********************************************************")
//
//    println("统计度数")
////    // Join the triangle counts with the usernames
////    val users = sc.textFile("E:\\graph\\spark-3.1.2-bin-hadoop3.2\\data\\graphx\\users.txt").map { line =>
////      val fields = line.split(",")
////      (fields(0).toLong, fields(1))
////    }
////    val triCountByUsername = users.join(fbTri).map { case (id, (username, tc)) =>
////      (username, tc)
////    }
////    // Print the result
////    println(triCountByUsername.collect().mkString("\n"))
//
////    println("出度")
////
////    facebook.outDegrees.foreach((it)=>{println(s"id ${it._1} out degree ${it._2}")})
////
////    println("入度")
////
////    facebook.inDegrees.foreach((it)=>{println(s"id ${it._1} in degree ${it._2}")})
////
////    println("总体度数")
////
////    facebook.degrees.foreach((it)=>{println(s"id ${it._1} total degree ${it._2}")})
////
////    println("子图")
//
////    val subFacebook = facebook.subgraph(vpred = (id, vd)=>{
////      println(s"vertex id ${id} vd ${vd}")
////      true})
////
////    subFacebook.vertices.collect.foreach(v => println(s"sub id ${v._1} vd ${v._2}"))
//    println(s"vertex count : ${facebook.vertices.count()}")
//    val count = facebook.degrees.count().toInt
//    val arrDegrees = facebook.degrees.take(count)
//    import org.jfree.data.xy.XYSeriesCollection
//    val dataset = new XYSeriesCollection
//    import org.jfree.data.xy.XYSeries
//    val xySeries = new XYSeries("degree")
//    arrDegrees.groupBy(it=>it._2).map(it=>(it._1,it._2.size)).toList.sortBy(it=>it._1).foreach(it=>{
//      xySeries.add(it._2,it._1)
//      println(s"degree is ${it._1} count ${it._2}")})
//    dataset.addSeries(xySeries)
//    val lineChart = ChartFactory.createXYLineChart("Degree count",
//      "degree", "Count of Vertex", dataset,
//      PlotOrientation.VERTICAL, true, true, false)
//
//    val plot = lineChart.getXYPlot
//    plot.getDomainAxis().setRange(0,50)
//    import org.jfree.chart.axis.NumberTickUnit
////    plot.getRangeAxis().setStandardTickUnits()
//    val peer = new ChartFrame("degree", lineChart, true)
//    peer.pack()
//    peer.setVisible(true);
//    facebook.edges.collect().foreach(it=>{})
//
////    peer.show()
//
////    arrDegrees.sortBy(_._2).foreach(it=>println(s"degrees id ${it._1} degree ${it._2}"))
////    val newGraph = facebook.mapVertices((id, vd)=> {
////      val de = 0
////      val degreeNode = arrDegrees.find(it => { it._1 == id})
////      degreeNode match {
////        case None => de
////        case Some(value) => value._2
////      }
////    })
////
////
////    newGraph.vertices.collect.foreach(v => println(s"newGraph id ${v._1} vd ${v._2}"))

    sc.stop()

  }
}


