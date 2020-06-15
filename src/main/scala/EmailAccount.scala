import net.liftweb.json.{DefaultFormats, _}


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

object ParseJsonArray extends App {
  implicit val formats = DefaultFormats

  // a JSON string that represents a list of EmailAccount instances
  val jsonString =
    """
    {"entities":["Amphibians","Anura","Apache Gora","Aquatic ecosystem","Diazooxonorleucine","Habitat","Human body","Natural Selection","Natural Springs","Population","Rana esculenta","Rana temporaria"],"journalVolume":"33","journalPages":"446-451","pmid":"","year":2004,"outCitations":[],"s2Url":"https://semanticscholar.org/paper/7e58b926bbbc122edeccb7cb4f7f68ca11480698","s2PdfUrl":"","id":"7e58b926bbbc122edeccb7cb4f7f68ca11480698","authors":[{"name":"M. V. Ushakov","ids":["2506899"]}],"journalName":"Russian Journal of Ecology","paperAbstract":"The marsh frog is a widespread and flexible species that mainly occupies various aquatic biotopes. In the Lipetsk oblast, these frogs avoid only closed forest water bodies and springs, and their habitats in the Central Russian Upland and the Oka–Don Lowland obviously differ from each other. According to Klimov et al. (1999), the number of these amphibians in the Oka– Don Lowland is greater. The comparison of morphological variation in frogs from these regions shows that the pressure of natural selection is greater in the Central Russian Upland (Vykhodtseva, 1992; Vykhodtseva and Klimov, 1993; Kovylina and Vykhodtseva, 1993; Klimov et al. , 1999), and this pressure determines the relationship between the demographic and morphological characteristics of the amphibian populations.","inCitations":[],"pdfUrls":[],"title":"Ecomorphological Characteristics of the Marsh Frog Rana ridibunda from the Galich'ya Gora Nature Reserve","doi":"10.1023/A:1020916001559","sources":[],"doiUrl":"https://doi.org/10.1023/A:1020916001559","venue":"Russian Journal of Ecology"}
    """

  // json is a JValue instance
  val json = parse(jsonString)
  val journals = (json \\ "entities").children
  println("\njournals:"+journals)

  for(p <- journals){
    println("p.values = " + p.values)
  }
//  val elements = (json \\ "emailAccount").children
//  for (acct <- elements) {
//    val m = acct.extract[EmailAccount]
//    println(s"Account: ${m.url}, ${m.username}, ${m.password}")
//    println(" Users: " + m.usersOfInterest.mkString(","))
//  }
}