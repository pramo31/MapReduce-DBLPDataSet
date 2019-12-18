**Running the project on AWS EMR**

*Prerequisites*
- Latest version of SBT should be installed in your system/cluster depending on where you will compile the project
- Create an aws emr instance with default configs
- Download the dblp.xml from the dblp public data set and place it in the emr cluster(hdfs). Commands to do that once you copy the dblp.xml.gz to cluster
    1) Go to location where the dblp.xml.gz is present and run 'gunzip dblp.xml.gz' 
    2) Run the following commands to create input folder in hdfs
        - hdfs dfs -mkdir /home
        - hdfs dfs -mkdir /home/hadoop
        - hdfs dfs -mkdir /home/hadoop/input
        - hdfs dfs -put {directory where dblp.xml is present} /home/hadoop/input/
- Upload the runnable jar to a aws s3 bucket (steps to create jar below).

**Note on Config File**
- *Before building the jar you can change the config file in ./src/main/resources as required. It contains input and outut paths.*
- *You can configure the map reduce jobs you want to run in MapReduce.conf file in src/main/resources of the project. Bin, no of top authors are configurable.*
- *You can change config file to run only particular map reduce jobs*


*Steps to run Map Reduce Job*
1) Go to the root folder Homework2
2) Enter the following command : 'sbt clean compile test' to run the unit tests
3) Run the command 'sbt assembly'. This will create a packaged jar file at this location -> ./target/scala-2.13/root-assembly-0.1.0.jar
4) Upload this jar to the created aws s3 bucket
5) Create a step in the AWS EMR Step with CUSTOM-JAR option and point it to the jar in the S3 bucket.
6) Output will be present in the following hdfs path -> /home/hadoop/output/MapreduceJob{jobNumber}/MapreduceJob{jobNumber}.csv [this path will also contain the part files]

*Note: An alternatve if running on some other Hadoop distribution os to copy the jar to the cluster via winscp and run it using hadoop commands, or put the binaries into the cluster and directly run the 'sbt run' command in a hadoop setup environment*

```
Map of job name to its functionalities:
Mapreducejob1 -> AuthorCount
Mapreducejob2 -> PublicationVenueStratification
Mapreducejob3 -> PublicationYearStratification
Mapreducejob4 -> Authorship
Mapreducejob5 -> MeanMediamStatistics
```

**Important Info**

**- YOUTUBE LINK : https://youtu.be/G1IfOOxiMpI**

**- MY Jar file in S3 Bucket (Public) In case you want to test it -> s3://mapred-pramodh/root-assembly-0.1.0.jar**

**- Uploaded the .csv files of the maprdeuce jobs in the ./output folder**

**MapReduce Implementation Document**

1) Author Count (Job 1) - 
    - Map : Here the Map job gets each xml as its input value. It parses the xml and iterates through all the xml tags and whenever it finds a start element woth tag name author increments the counter. Once the complete xml is traversed the counter gives the number of co-authors in that publication. The Map job writes this counter as the key and 1 as the value indicating a single 'n' co-author publication.
    - Reduce : The reduce jobs gets the key as number of co-authors and a list of 1's as the value. If we sum the list of 1's we get the total number of publications with 'n' autors.
    - A separate logic is written to create buckets (configurable using config file).
    
2) Author Count with Venue Stratification(Job 2)
    - Map : Here the Map job gets each xml as its input value. It parses the xml and iterates through all the xml tags and whenever it finds a start element woth tag name author increments the counter. It also notes down the parent xml tag and uses it as the key to get the publication venue from the 'VenueMap'. The complete xml is traversed the counter gives the number of co-authors in that publication. The Map job writes the concatenation of 'counter-venue' as the key and 1 as the value indicating a single 'n-venue' co-author publication.
    - Reduce : The reduce jobs gets the key as number of co-authors-venue combination and a list of 1's as the value. If we sum the list of 1's we get the total number of publications with 'n-venue' autors.
    - A separate logic is written to create buckets and stratification based on venue (configurable using config file).
    
3) Author Count with Year Stratification(Job 3)
    - Map : Here the Map job gets each xml as its input value. It parses the xml and iterates through all the xml tags and whenever it finds a start element woth tag name author increments the counter. It also notes down the year inside the year tag. The complete xml is traversed the counter gives the number of co-authors in that publication. The Map job writes the concatenation of 'counter-year' as the key and 1 as the value indicating a single 'n-year' co-author publication.
    - Reduce : The reduce jobs gets the key as number of co-authors-year combination and a list of 1's as the value. If we sum the list of 1's we get the total number of publications with 'n-year' autors.
    - A separate logic is written to create buckets and stratification based on year (configurable using config file).
4) Authorship(Job 4) - (Used a simple method (may not be right or accurate but atleast functionality wise its fine) to calculate authorship score as the given method seemed confusing)
    - Map : Here the Map job gets each xml as its input value. It parses the xml and iterates through all the xml tags and whenever it finds a start element woth tag name author increments the counter. Once the complete xml is traversed the counter gives the number of co-authors in that publication. If the counter is one, it indiactes a single author publication and we keep the authorship score as 1(value). If there are more than one author we give each author a score of 1/n. We also maintain a list of all authors in the publication. The Map job writes the key as each author in the list with value as 1 or 1/n depending on the number of authors.
    - Reduce : The reduce jobs gets the key as authors with their authorship in each publication (a list). Here we take an average of all the individual publication authorship scores and write the key as author and value as mapper. We then use a 'sorted mapper job' and sort based on authorship score
    - A separate logic is written to fetch top and least 100 authors (configurable using config file). Authors with lower score have collaborated more and are top 100 authors and the ones with least scores are the ones who have not collaborated much.
    
5) MeanMedianStats (Job 5 and 6)
	- Map : Here the Map job gets each xml as its input value. It parses the xml and iterates through all the xml tags and whenever it finds a start element woth tag name author it stores that value. I case of stratification it maps the first tag wih the venue using venue map. Once the complete xml is traversed the counter gives the number of co-authors in that publication. So we set this (value -1) as the number of co-authors for a particular author and send thema s key value pairs for each author. 
	Reduce : Here we get key (a particular author) and value list of number of co-authors for each publication. We sort this list and get the mid point as median and first and last element as min and max values. Then we get the sum of all elements by size of array as the average number of co-authors the author has worked with.


*Sorting Map Job - This is a simple job which sorts basedd on value ouput of another mapreduce job. Here the input key and value to the map job are interchanged and are written as output value and key. Then in the reduce the interchange is done again and to maintain the key and value pairs as in the input but now the pairs are sorted based on the input value of the KV pairs as the map jobs sets this value as key for its output and MapReduce framework sorts by key by default. A comparator can be used to change the order as ascending or descending as required.*