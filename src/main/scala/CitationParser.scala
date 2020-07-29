import java.io.FileInputStream
import java.util.Properties

import net.liftweb.json.{DefaultFormats, _}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration._
import scala.reflect.ClassTag

object CitationParser {

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

  def main(args: Array[String]): Unit = {

    //    Creating a spark configuration
    val conf = new SparkConf()

    //    Getting the properties for the environment
    val prop =
      try {
        val prop = new Properties()
        if (System.getenv("PATH").contains("Windows")) {
          prop.load(new FileInputStream("application-local.properties"))
          conf.setMaster("local[*]")
        } else if (System.getenv("PATH").contains("ichec")) {
          prop.load(new FileInputStream("application-ichec.properties"))
        } else {
          println("Issue identifying the environment, PATH is:", System.getenv("PATH"))
        }
        (prop)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          sys.exit(1)
      }

    conf.setAppName("Citation")

    //    Creating a spark context driver and setting log level to error
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //    Reading file to RDD
    println("Reading file to RDD...")
    val lines_orig = sc.textFile(prop.getProperty("file.path"))
    val lines = lines_orig.sample(false, prop.getProperty("sample.size").toDouble, 2)
    println("RDD created!")

    //    printing the top few rows
    //    lines.take(8).foreach(println)

    //    printing the number of records
    println(s"Number of entries in linesRDD is ${lines.count()}")

    //    extracting the data using lift json parser
    println("Extracting the data using lift json parser...")
    val publicationsRdd: RDD[Publication] = lines.map(line => {
      implicit val formats: DefaultFormats.type = DefaultFormats;
      parse(line).extract[Publication]
    }).cache()
    println("publicationRdd created!")

    //    checking for test flags
    val testMode = prop.getProperty("test.mode")
    val testEntity = prop.getProperty("test.entity")
    val testPrintMode = prop.getProperty("test.print.results")

    if (testMode.equals("true")) {
      if (testEntity.equals("publication")) {
        prfmChkPblctns(prop, sc, publicationsRdd, testPrintMode)
      }
      if (testEntity.equals("journal")) {
        prfmChkJrnls(prop, sc, publicationsRdd, testPrintMode)
      }
    } else {
      println("Please select one of the below options:")
      println("1 : Search for a Publication")
      println("2 : Search for a Journal")
      print("Enter here:")

      val input = scala.io.StdIn.readInt

      input match {
        case 1 => {
          //  Searching for publications
          val publicationGraph = getPublicationGraph(sc, publicationsRdd)

          println("Please enter the publication name or X to exit:")

          Iterator.continually(scala.io.StdIn.readLine)
            .takeWhile(_ != "X")
            .foreach {
              searchPublication =>
                publicationGraph.collectNeighbors(EdgeDirection.In).lookup((publicationGraph.vertices.filter {
                  journal => (journal._2.publicationName.equals(searchPublication))
                }.first)._1).map(publication => publication.sortWith(_._2.pr > _._2.pr).foreach(publication => println(publication._2)))
                println("Please enter the publication name or X to exit:")
            }

          //          println("Finding the most influential publications...")
          //
          //          println("creating rank: Started");
          //          val publicationRanks = publicationGraph.pageRank(0.0001).vertices
          //          println("creating rank: Completed");
          //  printing publication with top 10 ranks
          //          println("sorting and printing ranks");
          //          publicationRanks
          //            .join(publicationGraph.vertices)
          //            .sortBy(_._2._1, ascending = false) // sort by the rank
          //            .take(10) // get the top 10
          //            .foreach(publication => println(publication._2._2))
          //          println("printing ranks: Completed");
        }
        case 2 => {
          //  Searching for journal
          val jrnlDgrGrph = getJournalGraph(sc, publicationsRdd)

          println("Please enter the journal name or X to exit:")

          Iterator.continually(scala.io.StdIn.readLine)
            .takeWhile(_ != "X")
            .foreach {
              searchJournal =>
                jrnlDgrGrph.collectNeighbors(EdgeDirection.In).lookup((jrnlDgrGrph.vertices.filter {
                  journal => (journal._2.journalName.equals(searchJournal))
                }.first)._1).map(journal => journal.sortWith(_._2.pr > _._2.pr).foreach(journal => println(journal._2)))
                println("Please enter the journal name or X to exit:")
            }
        }
        case _ => println("Invalide Input! Exiting Now...!")
      }
    }

    //    drawGraph(degreeGraph)

    publicationsRdd.unpersist()
    sc.stop()
    println("end")
  }

  def prfmChkPblctns(prop: Properties, sc: SparkContext, publicationsRdd: RDD[Publication], testPrintMode: String): Unit = {
    val pblctnDgrGrph = getPublicationGraph(sc, publicationsRdd)
    val tstPblctnSze = prop.getProperty("test.publication.size").toInt
    val pblctnSmpls = sc.broadcast(pblctnDgrGrph.vertices.sortBy(_._2.pr, false).take(tstPblctnSze).drop(1))

    //    println("printing the sample publication names:")
    //    journalSamples.foreach(println)

    println("Querying for top " + tstPblctnSze + " publications with highest pagerank:")

    if (testPrintMode.equals("true")) {
      pblctnSmpls.value.foreach {
        srchPblctn =>
          val timedResult = time {
            println("Retrieving influential publications for: " + srchPblctn._2.publicationName)
            pblctnDgrGrph.collectNeighbors(EdgeDirection.In).lookup((pblctnDgrGrph.vertices.filter {
              publication => (publication._2.publicationName.equals(srchPblctn._2.publicationName))
            }.first)._1).map(publication => publication.sortWith(_._2.pr > _._2.pr).foreach(publication => println(publication._2)))
          }
          //        println("Time taken :" +{timedResult.durationInNanoSeconds})
          println(timedResult.durationInNanoSeconds.toMillis + ",")
      }
    } else {
      pblctnSmpls.value.foreach {
        srchPblctn =>
          val timedResult = time {
            //            println("Retrieving influential journals for: " + srchPblctn._2.publicationName)
            pblctnDgrGrph.collectNeighbors(EdgeDirection.In).lookup((pblctnDgrGrph.vertices.filter {
              publication => (publication._2.publicationName.equals(srchPblctn._2.publicationName))
            }.first)._1).map(publication => publication.sortWith(_._2.pr > _._2.pr))
          }
          //        println("Time taken :" +{timedResult.durationInNanoSeconds})
          println(timedResult.durationInNanoSeconds.toMillis + ",")
      }
    }
  }

  def prfmChkJrnls(prop: Properties, sc: SparkContext, publicationsRdd: RDD[Publication], testPrintMode: String): Unit = {
    val jrnlDgrGrph = getJournalGraph(sc, publicationsRdd)
    val tstJrnlSze = prop.getProperty("test.journal.size").toInt
    val journalSamples = sc.broadcast(jrnlDgrGrph.vertices.sortBy(_._2.pr, false).take(tstJrnlSze).drop(1))

    //    println("printing the sample journal names:")
    //    journalSamples.foreach(println)

    println("Querying for top " + tstJrnlSze + " journals with highest pagerank:")

    if (testPrintMode.equals("true")) {
      journalSamples.value.foreach {
        searchJournal =>
          val timedResult = time {
            println("Retrieving influential journals for: " + searchJournal._2.journalName)
            jrnlDgrGrph.collectNeighbors(EdgeDirection.In).lookup((jrnlDgrGrph.vertices.filter {
              journal => (journal._2.journalName.equals(searchJournal._2.journalName))
            }.first)._1).map(journal => journal.sortWith(_._2.pr > _._2.pr).foreach(journal => println(journal._2)))
          }
          //        println("Time taken :" +{timedResult.durationInNanoSeconds})
          println(timedResult.durationInNanoSeconds.toMillis + ",")
      }
    } else {
      journalSamples.value.foreach {
        searchJournal =>
          val timedResult = time {
            jrnlDgrGrph.collectNeighbors(EdgeDirection.In).lookup((jrnlDgrGrph.vertices.filter {
              journal => (journal._2.journalName.equals(searchJournal._2.journalName))
            }.first)._1).map(journal => journal.sortWith(_._2.pr > _._2.pr))
          }
          //        println("Time taken :" +{timedResult.durationInNanoSeconds})
          println(timedResult.durationInNanoSeconds.toMillis + ",")
      }
    }
  }

  def getJournalGraph(sc: SparkContext, publicationsRdd: RDD[Publication]) = {

    //    create journal RDD vertices with publications
    val publicationsRdd_nonempty = publicationsRdd.filter(publication => publication.journalName != "")
    val publicationGroups = publicationsRdd_nonempty.groupBy(publication => publication.journalName)
    val journalRDD: RDD[Journal] = publicationGroups.map(publication => Journal(publication._1, publication._2.toList)).distinct

    println("creating journal vertices...")
    val journalWithIndex = journalRDD.zipWithIndex()
    val journalVertices = journalWithIndex.map { case (k, v) => (v, k) }

    val journalVertices2 = journalVertices.map {
      case (k, v) => (k, v.journalName)
    }

    val journalPublicDict = sc.broadcast(journalVertices.flatMap {
      case (jid, journal) =>
        journal.publications.map(p => (p.id, jid))
    }.collectAsMap())

    println("journal vertices created!")

    val nocitation = "nocitation"

    val jrnlEdgsWthDplcts1 = journalVertices.flatMap {
      case (jid, journal) =>
        journal.publications.flatMap(
          publication => publication.outCitations.map(
            outCitation => (jid,
              journalPublicDict.value.getOrElse(outCitation, -1.toLong)
              , 1)))
    }

    val jrnlEdgsWthDplcts2 = jrnlEdgsWthDplcts1.filter(dupEdgs1 => dupEdgs1._1 != dupEdgs1._2).filter(dupEdgs1 => dupEdgs1._2 != -1)

    val zeroVal = 0
    val addToCounts = (acc: Int, ele: Int) => (acc + ele)
    val sumPartitionCounts = (acc1: Int, acc2: Int) => (acc1 + acc2)

    println("creating journal edges...")
    val journalEdges = jrnlEdgsWthDplcts2.map(dupEdgs2 => ((dupEdgs2._1, dupEdgs2._2), dupEdgs2._3)).aggregateByKey(0)(addToCounts, sumPartitionCounts).map(unqEdgs => Edge(unqEdgs._1._1, unqEdgs._1._2, unqEdgs._2))
    println("journal edges created!")

    println("creating journal graph with degrees...")
    //    creating journal graph with degrees
    val JournalGraph = Graph(journalVertices2, journalEdges, nocitation).mapVertices {
      case (jid, jname) =>
        JournalWithDegrees(jid, jname, 0, 0, 0.0)
    }

    val inDegrees = JournalGraph.inDegrees
    val outDegrees = JournalGraph.outDegrees
    val pageRank = JournalGraph.pageRank(0.0001).vertices
    val jrnlDgrGrph: Graph[JournalWithDegrees, Int] = JournalGraph.outerJoinVertices(inDegrees) {
      (jid, j, inDegOpt) => JournalWithDegrees(j.jid, j.journalName, inDegOpt.getOrElse(0), j.outDeg, j.pr)
    }.outerJoinVertices(outDegrees) {
      (jid, j, outDegOpt) => JournalWithDegrees(j.jid, j.journalName, j.inDeg, outDegOpt.getOrElse(0), j.pr)
    }.outerJoinVertices(pageRank) {
      (jid, j, prOpt) => JournalWithDegrees(j.jid, j.journalName, j.inDeg, j.outDeg, prOpt.getOrElse(0))
    }

    println("journal graph with degrees and page rank added")
    jrnlDgrGrph.cache()
  }

  //  function to identify the time taken
  def time[R](block: => R): TimedResult[R] = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val duration = t1 - t0
    TimedResult(result, duration nanoseconds)
  }

  //  define the method to get publication graph
  def getPublicationGraph(sc: SparkContext, publicationsRdd: RDD[Publication]): Graph[PublicationWithDegrees, Int] = {

    val pblctnsWthIndxRdd = publicationsRdd.zipWithIndex().map { case (k, v) => (v, k) }

    val pblctnIdDict = sc.broadcast(pblctnsWthIndxRdd.map(p => (p._2.id, p._1)).collectAsMap())

    //    create publication RDD vertices with ID and Name
    println("creating publication vertices...")
    val publicationVertices: RDD[(Long, String)] = publicationsRdd.map(publication => (pblctnIdDict.value.getOrElse(publication.id, -1.toLong), publication.title)).distinct
    println("publication vertices created!")

    //     Defining a default vertex called nocitation
    val nocitation = "nocitation"

    println("creating citations...")
    val citations = publicationsRdd.map(publication => ((pblctnIdDict.value.getOrElse(publication.id, -1.toLong), publication.outCitations), 1)).distinct
    println("citations created!")

    println("creating citation edges with outCitations and inCitations...")
    //    creating citation edges with outCitations and inCitations
    val citationEdges = citations.flatMap {
      case ((id, outCitations), num) =>
        outCitations.map(outCitation => Edge(id, pblctnIdDict.value.getOrElse(outCitation, -1.toLong), num))
    }
    println("citation edges created!")

    println("creating publication graph with degrees...")
    //    creating publication graph with degrees
    val publicationGraph = Graph(publicationVertices, citationEdges, nocitation).mapVertices {
      case (pid, pname) =>
        PublicationWithDegrees(pid, pname, 0, 0, 0.0)
    }

    val inDegrees = publicationGraph.inDegrees
    val outDegrees = publicationGraph.outDegrees
    val pageRank = publicationGraph.pageRank(0.0001).vertices

    val pblctnDgrGrph: Graph[PublicationWithDegrees, Int] = publicationGraph.outerJoinVertices(inDegrees) {
      (pid, p, inDegOpt) => PublicationWithDegrees(p.id, p.publicationName, inDegOpt.getOrElse(0), p.outDeg, p.pr)
    }.outerJoinVertices(outDegrees) {
      (pid, p, outDegOpt) => PublicationWithDegrees(p.id, p.publicationName, p.inDeg, outDegOpt.getOrElse(0), p.pr)
    }.outerJoinVertices(pageRank) {
      (pid, p, prOpt) => PublicationWithDegrees(p.id, p.publicationName, p.inDeg, p.outDeg, prOpt.getOrElse(0))
    }

    println("publication graph with degrees and page rank added")
    pblctnDgrGrph.cache()
  }

  //  define the journal schema
  case class Journal(
                      journalName: String,
                      publications: List[Publication]
                    )

  //  define the publication schema
  case class Publication(
                          entities: List[String],
                          journalVolume: Option[String],
                          journalPages: String,
                          pmid: String,
                          year: Option[Int],
                          outCitations: List[String],
                          s2Url: String,
                          s2PdfUrl: String,
                          id: String,
                          authors: List[Authors],
                          journalName: String,
                          paperAbstract: String,
                          inCitations: List[String],
                          pdfUrls: List[String],
                          title: String,
                          doi: String,
                          sources: List[String],
                          doiUrl: String,
                          venue: String)

  //  define the journal graph schema with degrees
  case class JournalWithDegrees(
                                 jid: VertexId,
                                 journalName: String,
                                 inDeg: Int,
                                 outDeg: Int,
                                 pr: Double
                               )

  //  define the publication graph schema with degrees
  case class PublicationWithDegrees(
                                     id: VertexId,
                                     publicationName: String,
                                     inDeg: Int,
                                     outDeg: Int,
                                     pr: Double
                                   )

  //  define the author schema
  case class Authors(
                      name: String,
                      ids: List[String]
                    )

  case class TimedResult[R](result: R, durationInNanoSeconds: FiniteDuration)

}