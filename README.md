# Citation Network Analysis using GraphX

The popularity of the scientific publications keeps on increasing day by day and the number of publications is also immensely increasing. Thus, it becomes more challenging for any researcher to search for a topic, review the literature, follow research trends in that field. The use of online search engines help to a certain extent but the number of results is vast. In the past, many solutions are proposed to analyse the document similarities using Natural Language Processing(NLP) which makes use of pattern matching but these results are not so accurate.

In this dissertation, we propose a system to analyse the document similarities by making use of the connectedness between them which can be known by using the citations and references that are given by the original authors of the document. Our system makes use of the semantic scholar publication data to form a citation network graph. This network thus gives a much better insight into a publication among a network of publications. The graph that is constructed with this approach contains publications as nodes of the graph and citations as directed edges of the graph. Besides, this approach helps in searching for publications, journals as a separate query and dynamic graphs for each of the entities publications, journal are created. The interactive system is cable of getting the most influential publications/journals connected to any selected publication. 

Another key aspect of our dissertation is the use of emerging big data analytics system, i.e. \enquote{Apache Spark} platform and \enquote{GraphX} framework for citation network analysis. The project was implemented in national supercomputing infrastructure (ICHEC) with the 47 GB raw data of publication citation network and references.

## Installation

### For Windows:

Please download and install the IntelliJ IDEA version 11.0.7 from: https://www.jetbrains.com/idea/download/#section=windows

Please download and install Scala version 2.11.7 from: https://www.scala-lang.org/download/

Please download and install java version Java SE 8u261 from: https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html

Please download and install Spark version 2.4.3 from: https://spark.apache.org/downloads.html 
Spark installation steps can be found here: https://phoenixnap.com/kb/install-spark-on-windows-10

### For Linux:

Please download and install Scala version 2.11.7 from: https://www.scala-lang.org/download/

Please download and install java version Java SE 8u261 from: https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html

Please download and install Spark version 2.4.3 from: https://spark.apache.org/downloads.html

Please download and install SBT build tool using sdkman as below:

#### install sdkman
```bash
curl -s "https://get.sdkman.io" | bash
```

#### source sdkman
```bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
```

#### install java and sbt
```bash
sdk list java
sdk install java 11.0.7.hs-adpt
sdk install sbt
```

### install scala for hello world program
```bash
mkdir test && cd ./test
sbt new scala/hello-world.g8
# when prompted, give name hello-world
```

#### run hello-world interactively
```bash
cd ./hello-world
sbt
# in the sbt console
~run
exit
```
##  Clone

The code can be cloned from the GIT URL: https://github.com/ramesh-suragam/CNA.git

In linux use the below commands to clone the repository:
```bash
git clone https://github.com/ramesh-suragam/CNA.git
```

##  Building the code

In linux:
Building the jar file for the project also requires us to all the dependencies inside it to create a fat jar. We use the sbt assembly plugin for this. Please follow the below steps to activate this plugin:

Uncomment the below assembly plugin lines from build.sbt:
```bash
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
```
Create a new file plugins.sbt under project folder:
```bash
touch project/plugins.sbt
echo "addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")" > project/plugins.sbt
```
To build the jar file use the below:
```bash
sbt assembly
```
The new jar file gets created under: /target/scala-2.11/CNA-assembly-0.1.jar

##  Deployment
##  Running the tests

##  Authors
Ramesh Suragam












## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.
