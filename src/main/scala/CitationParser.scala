import org.apache.spark.{SparkConf, SparkContext}
import net.liftweb.json.{DefaultFormats, _}

import scala.collection.mutable.ArrayBuffer

case class Journal(
                    entities: List[String],
                    journalVolume: String,
                    journalPages: String,
                    pmid: String,
                    year: Int,
                    outCitations: List[String],
                    s2Url: String,
                    s2PdfUrl: String,
                    id: String,
                    authors: Authors,
                    journalName: String,
                    paperAbstract: String,
                    inCitations: List[String],
                    pdfUrls: List[String],
                    title: String,
                    doi: String,
                    sources: List[String],
                    doiUrl: String,
                    venue: String)

case class Authors(
                    name: String,
                    ids: List[String]
                  )

object CitationParser extends App {

  implicit val formats = DefaultFormats

  def getJournals(journalJsonString: String): Array[Journal]={
    var entityValues=List[String]()
    var journalVolumeValues=String
    var journalPageValues=String
    var pmidValues=String
    var yearValues=Int
    var outCitationValues=List[String]()
    var s2UrlValues=String
    var s2PdfUrlValues=String
    var idValues=String
    var authorValues=ArrayBuffer[Authors]()
    var journalNameValues=String
    var paperAbstractValues=String
    var inCitationValues=List[String]()
    var pdfUrlValues=List[String]()
    var titleValues=String
    var doiValues=String
    var sourcesValues=List[String]()
    var doiUrlValues=String
    var venueValues=String

    val json = parse(journalJsonString)

    val entities = (json \\ "entities").children
    for(p <- entities){
      entityValues+=p.extract[String]
    }
    val journalVolume = (json \\ "journalVolume").children
    for(p <- journalVolume){
      journalVolumeValues+=p.extract[String]
    }
    val journalPages = (json \\ "journalPages").children
    for(p <- journalPages){
      journalPageValues +=p.extract[String]
    }
    val pmid = (json \\ "pmid").children
    for(p <- pmid){
      pmidValues +=p.extract[String]
    }
    val year = (json \\ "").children
    for(p <- year){
      val yearValues =p.extract[Int]
    }
    val outCitations = (json \\ "outCitations").children
    for(p <- outCitations){
      outCitationValues +=p.extract[String]
    }
    val s2Url = (json \\ "s2Url").children
    for(p <- s2Url){
      s2UrlValues +=p.extract[String]
    }
    val s2PdfUrl = (json \\ "s2PdfUrl").children
    for(p <- s2PdfUrl){
      s2PdfUrlValues +=p.extract[String]
    }
    val id = (json \\ "id").children
    for(p <- id){
      idValues +=p.extract[String]
    }
    val authors = (json \\ "authors").children
    for(p <- authors){
      authorValues +=p.extract[Authors]
    }
    val journalName = (json \\ "journalName").children
    for(p <- journalName){
      journalNameValues +=p.extract[String]
    }
    val paperAbstract = (json \\ "paperAbstract").children
    for(p <- paperAbstract){
      paperAbstractValues +=p.extract[String]
    }
    val inCitations = (json \\ "inCitations").children
    for(p <- inCitations){
      inCitationValues +=p.extract[String]
    }
    val pdfUrls = (json \\ "pdfUrls").children
    for(p <- pdfUrls){
      pdfUrlValues +=p.extract[String]
    }
    val title = (json \\ "title").children
    for(p <- title){
      titleValues +=p.extract[String]
    }
    val doi = (json \\ "doi").children
    for(p <- doi){
      doiValues +=p.extract[String]
    }
    val sources = (json \\ "sources").children
    for(p <- sources){
      sourcesValues +=p.extract[String]
    }
    val doiUrl = (json \\ "doiUrl").children
    for(p <- doiUrl){
      doiUrlValues +=p.extract[String]
    }
    val venue = (json \\ "venue").children
    for(p <- venue){
      venueValues +=p.extract[String]
    }
Journal(entityValues,journalVolumeValues,journalPageValues,pmidValues,yearValues,outCitationValues,s2UrlValues,s2PdfUrlValues,idValues,authorValues,journalNameValues,paperAbstractValues,inCitationValues,pdfUrlValues,titleValues,doiValues,sourcesValues,doiUrlValues,venueValues)
  }

  def parseLine(jsonString: String) = {
    val json = parse(jsonString)
    val journals = (json \\ "entities").children
    println("\njournals:"+journals)

    for(p <- journals){
      println("p.values = " + p.values)
    }
  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]","Citation")
    sc.setLogLevel("ERROR")

    val lines = sc.textFile("file:///All Items Offline/Sem2/CS648 Project/sample_data/s2-corpus-00/s2-corpus-00")

    val rdd = lines.map(x => parse(x).extract[Journal])
  }
}
