import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.graphx.Graph

import scala.concurrent.duration._
import scala.reflect.ClassTag

case class TimedResult[R](result: R, durationInNanoSeconds: FiniteDuration)

object Utils {

  def getProperties(): Properties = {
    try {
      val prop = new Properties()
      if (System.getenv("PATH").contains("Windows")) {
        prop.load(new FileInputStream("application-local.properties"))
        //          conf.setMaster("local[*]")
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
