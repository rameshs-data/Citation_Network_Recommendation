#  Citation Network Analysis using GraphX

The popularity of the scientific publications keeps on increasing day by day and the number of publications is also immensely increasing. Thus, it becomes more challenging for any researcher to search for a topic, review the literature, follow research trends in that field. The use of online search engines help to a certain extent but the number of results is vast. In the past, many solutions are proposed to analyse the document similarities using Natural Language Processing(NLP) which makes use of pattern matching but these results are not so accurate.

In this dissertation, we propose a system to analyse the document similarities by making use of the connectedness between them which can be known by using the citations and references that are given by the original authors of the document. Our system makes use of the semantic scholar publication data to form a citation network graph. This network thus gives a much better insight into a publication among a network of publications. The graph that is constructed with this approach contains publications as nodes of the graph and citations as directed edges of the graph. Besides, this approach helps in searching for publications, journals as a separate query and dynamic graphs for each of the entities publications, journal are created. The interactive system is cable of getting the most influential publications/journals connected to any selected publication. 

Another key aspect of our dissertation is the use of emerging big data analytics system, i.e. \enquote{Apache Spark} platform and \enquote{GraphX} framework for citation network analysis. The project was implemented in national supercomputing infrastructure (ICHEC) with the 47 GB raw data of publication citation network and references.

##  ON WINDOWS:

### Installation:

* Please download and install GIT from: https://git-scm.com/download/win

* Please download and install the IntelliJ IDEA version 11.0.7 from: https://www.jetbrains.com/idea/download/#section=windows

* Please download and install Scala version 2.11.7 from: https://www.scala-lang.org/download/

* Please download and install java version Java SE 8u261 from: https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html

* Please download and install Spark version 2.4.3 from: https://spark.apache.org/downloads.html 

* Spark installation steps can be found here: https://phoenixnap.com/kb/install-spark-on-windows-10

### Clone:

The code can be cloned from the GIT URL: https://github.com/ramesh-suragam/CNA.git

### Setting the workspace:

Please follow these sterps to setup the workspace on IntelliJ using: https://www.jetbrains.com/help/idea/set-up-a-git-repository.html#ignore-files

### Running the Code:

#### prerequisites:

We can run the code either in interative mode or test mode:

##### Interactive Mode:
Change the below tag from application-local.properties file to false for interactive mode.
Change the file.path and sample size to set them.
```bash
test.mode = false
file.path = file:///ichec/home/users/rameshs999/PubCiteNetAnalysis/s2-corpus-*
sample.size = 1
```
##### Test Mode:
Change the test.mode tag from application-local.properties file to true for test mode:
Use the test.entity.publication flag to test for publication, test.entity.journal flag to test for journal or set both flags to true to test both.
Use test.publication.size to set the number of publications to test for, test.journal.size flag to set the number of journals to test for.
Use test.print.results flag to see or skip test debug statements.

```bash
### Test Mode
test.mode = true
test.entity.publication = true
test.entity.journal = true
### Test Size
test.publication.size=500
test.journal.size = 500
### Test Output
test.print.results = false
```
Use the below flog from application-local.properties file to set the input data path:
```bash
### 
### File Input details
file.path = file:///ichec/home/users/rameshs999/PubCiteNetAnalysis/s2-corpus-000
sample.size = 1
```
The steps to run the code are same after setting the prerequisites as below:

* From the Run menu, select Edit configurations

* Click the + button and select sbt Task.

* Name it Run the program.

* In the Tasks field, type ~run. The ~ causes sbt to rebuild and rerun the project when you save changes to a file in the project.

* Click OK.

* On the Run menu. Click Run ‘Run the program’.

* In the code, change 75 to 61 and look at the updated output in the console.

##  ON LINUX:

### Installation:

Please download and install GIT from: https://git-scm.com/download/linux

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

#### install scala for hello world program
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

### Clone
The code can be cloned from the GIT URL: https://github.com/ramesh-suragam/CNA.git

In linux use the below commands to clone the repository:
```bash
git clone https://github.com/ramesh-suragam/CNA.git
```

#### Running in Interactive Mode:
#####  prerequisites
Change the below tag from application-ichec.properties file to false for interactive mode
```bash
test.mode = false
```
#### execution of the code
From the source directory containing build.sbt file run the following command to start the interactive mode and follow the on screen options:
```bash
sbt run
```

### Running in Test Mode:
####  prerequisites
Change the test.mode tag from application-ichec.properties file to true for test mode:
Use the test.entity.publication flag to test for publication, test.entity.journal flag to test for journal or set both flags to true to test both.
Use test.publication.size to set the number of publications to test for, test.journal.size flag to set the number of journals to test for.
Use test.print.results flag to see or skip test debug statements.
```bash
### Test Mode
test.mode = true
test.entity.publication = true
test.entity.journal = true
### Test Size
test.publication.size=500
test.journal.size = 500
### Test Output
test.print.results = false
```
Use the below flog from application-ichec.properties file to set the input data path:
```bash
### 
### File Input details
file.path = file:///ichec/home/users/rameshs999/PubCiteNetAnalysis/s2-corpus-*
sample.size = 1
```
#### execution of the code
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

Use the generated jar file to submit a spark-submit job on any Spark cluster. This code is built and executed on Spark Standalone cluster which are built using additional shell scripts which are subjective to the ICHEC cluster environment and there lies no dependencies of this code on those scripts. Below is a sample code to trigger spark-submit from command line. The arguments are dependent on the cluster being used.
```bash
spark-submit --driver-memory 140G --executor-memory 22G --total-executor-cores 385 --num-executors 77 --executor-cores 5 --conf spark.driver.maxResultSize=3g --class "CitationParser" --master "spark://10.54.$1:7077" /ichec/home/users/rameshs999/CNA/CNA9/CNA/target/scala-2.11/CNA-assembly-0.1.jar
```

##  Authors
Ramesh Suragam

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.
