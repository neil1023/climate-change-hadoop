hdfs dfs -rm -r /avg_temp_cities
hdfs dfs -rm -r /avg_temp_cities/output
sudo rm -rf classes/*
sudo rm AvgTempCities.jar
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main -d classes AvgTempCities.java
jar cf AvgTempCities.jar classes/*.class
/usr/local/hadoop/bin/hadoop jar AvgTempCities.jar AvgTempCities /cities /avg_temp_cities/output
