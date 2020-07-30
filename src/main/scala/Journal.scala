import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD

//  define the journal schema
case class Journal(
                    journalName: String,
                    publications: List[Publication]
                  )

//  define the journal graph schema with degrees
case class JournalWithDegrees(
                               jid: VertexId,
                               journalName: String,
                               inDeg: Int,
                               outDeg: Int,
                               pr: Double
                             )

object Journal {
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

  def prfmChkJrnls(prop: Properties, sc: SparkContext, publicationsRdd: RDD[Publication], testPrintMode: String): Unit = {
    val jrnlDgrGrph = getJournalGraph(sc, publicationsRdd)
    val tstJrnlSze = prop.getProperty("test.journal.size").toInt
    val journalSamples = jrnlDgrGrph.vertices.sortBy(_._2.pr, false).take(tstJrnlSze)

    //    println("printing the sample journal names:")
    //    journalSamples.foreach(println)

    println("Querying for top " + tstJrnlSze + " journals with highest pagerank:")

    if (testPrintMode.equals("true")) {
      journalSamples.foreach {
        searchJournal =>
          val timedResult = Utils.time {
            println("Retrieving influential journals for: " + searchJournal._2.journalName)
            jrnlDgrGrph.collectNeighbors(EdgeDirection.In).lookup((jrnlDgrGrph.vertices.filter {
              journal => (journal._2.journalName.equals(searchJournal._2.journalName))
            }.first)._1).map(journal => journal.sortWith(_._2.pr > _._2.pr).foreach(journal => println(journal._2)))
          }
          //        println("Time taken :" +{timedResult.durationInNanoSeconds})
          println(timedResult.durationInNanoSeconds.toMillis + ",")
      }
    } else {
      journalSamples.foreach {
        searchJournal =>
          val timedResult = Utils.time {
            jrnlDgrGrph.collectNeighbors(EdgeDirection.In).lookup((jrnlDgrGrph.vertices.filter {
              journal => (journal._2.journalName.equals(searchJournal._2.journalName))
            }.first)._1).map(journal => journal.sortWith(_._2.pr > _._2.pr))
          }
          //        println("Time taken :" +{timedResult.durationInNanoSeconds})
          println(timedResult.durationInNanoSeconds.toMillis + ",")
      }
    }
  }
}
