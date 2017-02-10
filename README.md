1) Make sure code is right
2) mvn clean && mvn compile && mvn package
3) Upload jar in target/ to edge node
4) Upload h2o-genmodel.jar to edge node
5) hadoop jar mr-pojo-1.0-SNAPSHOT.jar MrPojo -libjars h2o-genmodel.jar inputfile.csv /user/nick/outputdir
