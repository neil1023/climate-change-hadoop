hdfs dfs -rm -r /avg_temp_cities
sudo rm -rf classes
sudo rm AvgTempCities.jar
mkdir classes
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main -d classes AvgTempCities.java
jar cf AvgTempCities.jar classes/*.class
/usr/local/hadoop/bin/hadoop jar AvgTempCities.jar AvgTempCities /cities /avg_temp_cities/output_w_avgs
hadoop fs -get /avg_temp_cities/output/part-r-00000
mv part-r-00000 output_results
python3 computeDeltas.py
