# cdranalytics

Prerequisite:-
Install cloudera VM:
https://www.cloudera.com/downloads/quickstart_vms/5-13.html

Start the VM and do the fallowing steps:

Run steps:-

- Download the source code :
https://github.com/divang/cdranalytics

- Open terminal 
    
    - Compile the code
        - cd /home/cloudera/Downloads/cdranalytics-master/src/main/java
        - javac -cp .:/usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hbase/* com/telecom/cdranalytics/filters/*.java
        - javac -cp .:/usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hbase/* com/telecom/cdranalytics/*.java

    - Make a jar
        - cd /home/cloudera/Downloads/cdranalytics-master/src/main/java
        - jar -cvf cdrAnalytics.jar com/telecom/cdranalytics/*.class com/telecom/cdranalytics/filters/*.class

    - HDFS
        - cd /home/cloudera/Downloads/cdranalytics-master
        - hdfs dfs -copyFromLocal cdr.txt .

    - Hbase
        - hbase shell
        - create 'promotedFreePackUsers', 'callerStatistic'
        - list
        - scan 'promotedFreePackUsers'

    - Hadoop Map/Reduce Job submission
        - cd /home/cloudera/Downloads/cdranalytics-master/src/main/java
        - export HADOOP_CLASSPATH=/usr/lib/hadoop/*:/usr/lib/hbase/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hbase/lib/*
        - hadoop jar cdrAnalytics.jar com.telecom.cdranalytics.STDSubscribers cdr.txt

    - Job has done
        - hbase shell
        - scan 'promotedFreePackUsers'

    - Show the progress report
        - http://quickstart.cloudera:8088/cluster
       
    - Reports All records of Users:
        - comiple
            - javac -cp .:/usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hbase/*
com/telecom/cdranalytics/AllUserRecords.java
        - To view the Report:
            - java -cp .:/usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hbase/*:/usr/lib/hbase/lib/*
 com.telecom.cdranalytics.AllUserRecords

    - Get Free pack eligible Users from HBase database: 
        - compile
            - javac -cp .:/usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hbase/*
com/telecom/cdranalytics/FreePackUser.java
        - To view the eligible User for free pack:
            - java -cp .:/usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hbase/*
com.telecom.cdranalytics.FreePackUser 500
