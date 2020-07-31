import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration._
import scala.reflect.ClassTag

case class TimedResult[R](result: R, durationInNanoSeconds: FiniteDuration)

object Utils {

  def getProperties(): Properties = {
    try {
      val prop = new Properties()
      if (System.getenv("PATH").contains("Windows")) {
        prop.load(new FileInputStream("application-local.properties"))
      } else if (System.getenv("PATH").contains("ichec")) {
        prop.load(new FileInputStream("application-ichec.properties"))
      } else {
        println("Issue identifying the environment, PATH is:", System.getenv("PATH"))
      }
      prop
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    }
  }

  //  method to print task metrics
  def prntGrphTstMtrcs(spark: SparkSession): Unit = {
    println("Looking for metrics tables...")
    spark.sql("show tables").show
    println("Printing metrics table...")
    spark.sql("select * from PerfTaskMetrics").show
    println("Printing Top tasks based on Execution time and CPU time...")
    val perfTaskMetrics = spark.sql("select stageId, count(*) as count, sum(executorRunTime) as sum_exctrRnTme, sum(executorCpuTime) as sum_exctrCpuTme " +
      "from PerfTaskMetrics " +
      "group by stageId " +
      "order by sum_exctrRnTme desc, sum_exctrCpuTme desc limit 1000")
    perfTaskMetrics.show
    //        perfTaskMetrics.repartition(1).write.format("com.databricks.spark.csv").save(prop.getProperty("test.task.metrics")) //commenting code to save the results to file
  }

  def prntHdngLne(heading: String): Unit = {

    val headingUpper = heading.toUpperCase
    val wordSize = headingUpper.length
    val preSpace = " " * ((40 - wordSize - 6) / 2)
    val postSpace = if (wordSize % 2 == 0) {
      " " * ((40 - wordSize - 6) / 2)
    } else {
      " " * (((40 - wordSize - 6) / 2) + 1)
    }
    println("##############################################################################################################")
    if (wordSize > 40) {
      print("##################")
    } else {
      print("###################################")
    }
    print(preSpace)
    print(s"-- $headingUpper --")
    print(postSpace)
    if (wordSize > 35) {
      println("##################")
    } else {
      println("###################################")
    }
    println("##############################################################################################################")
  }

  def prntHdngEndLne(): Unit = {
    println("##############################################################################################################")
    println()
  }

  def prntSbHdngLne(subHeading: String): Unit = {

    val headingUpper = subHeading.toUpperCase
    val wordSize = headingUpper.length
    val preSpace = " " * ((46 - wordSize - 6) / 2)
    val postSpace = if (wordSize % 2 == 0) {
      " " * ((46 - wordSize - 6) / 2)
    } else {
      " " * (((46 - wordSize - 6) / 2) + 1)
    }
    println("##########################################################################")
    if (wordSize > 40) {
      print("##########")
    } else {
      print("##############")
    }
    print(preSpace)
    print(s"-- $headingUpper --")
    print(postSpace)
    if (wordSize > 35) {
      println("##########")
    } else {
      println("##############")
    }
    println("##########################################################################")
  }

  def prntSbHdngEndLne(): Unit = {
    println("##########################################################################")
    println()
  }
  // method to get highest degree vertex in graphs
  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }

  //  function to identify the time taken
  def time[R](block: => R): TimedResult[R] = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val duration = t1 - t0
    TimedResult(result, duration nanoseconds)
  }

  def drawGraph[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]) = {

    val u = java.util.UUID.randomUUID
    val v = g.vertices.collect.map(_._1)
    println(
      """%html
<div id='a""" + u +
        """' style='width:960px; height:500px'></div>
<style>
.node circle { fill: gray; }
.node text { font: 10px sans-serif;
     text-anchor: middle;
     fill: white; }
line.link { stroke: gray;
    stroke-width: 1.5px; }
</style>
<script src="//d3js.org/d3.v3.min.js"></script>
<script>
.var width = 960, height = 500;
var svg = d3.select("#a""" + u +
        """").append("svg")
.attr("width", width).attr("height", height);
var nodes = [""" + v.map("{id:" + _ + "}").mkString(",") +
        """];
var links = [""" + g.edges.collect.map(
        e => "{source:nodes[" + v.indexWhere(_ == e.srcId) +
          "],target:nodes[" +
          v.indexWhere(_ == e.dstId) + "]}").mkString(",") +
        """];
var link = svg.selectAll(".link").data(links);
link.enter().insert("line", ".node").attr("class", "link");
var node = svg.selectAll(".node").data(nodes);
var nodeEnter = node.enter().append("g").attr("class", "node")
nodeEnter.append("circle").attr("r", 8);
nodeEnter.append("text").attr("dy", "0.35em")
 .text(function(d) { return d.id; });
d3.layout.force().linkDistance(50).charge(-200).chargeDistance(300)
.friction(0.95).linkStrength(0.5).size([width, height])
.on("tick", function() {
link.attr("x1", function(d) { return d.source.x; })
  .attr("y1", function(d) { return d.source.y; })
  .attr("x2", function(d) { return d.target.x; })
  .attr("y2", function(d) { return d.target.y; });
node.attr("transform", function(d) {
return "translate(" + d.x + "," + d.y + ")";
});
}).nodes(nodes).links(links).start();
</script>
 """)
  }
}
