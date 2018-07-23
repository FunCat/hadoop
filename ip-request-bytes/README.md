# Ip request bytes

The HadoopIpBytes application find the average bytes per request by IP and total bytes by IP.
The input format of the data is:

Example: 
> ip13 - - [24/Apr/2011:04:41:53 -0400] "GET /logs/access_log.3 HTTP/1.1" 200 4846545 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

where: 
* ip3 - the number of IP, 
* 200 - HTTP response status, 
* 4846545 - bytes, Mozilla/5.0 - browser.

The output format represents as Sequence file compressed with Snappy. The text inside the Sequence file
combine in CSV format:
> IP,average_bytes,total_bytes

Example:
> ip1,25819.02,2762635

For running application you need to compile it by the following command:
```gradle
gradle :ip-request-bytes:clean :ip-request-bytes:build
```

After that you need to run the application on the hadoop environment. Before the start put
the input file to the hdfs and check that the directory for the output doesn't exist.
Run the following command:
```
hadoop jar [path_to_the_jar] [path_to_the_input_file] [path_to_the_output_directory]
```
Example:
```
hadoop jar ip-request-bytes-1.0-SNAPSHOT.jar /input /output
```

To read the output Sequence file use the following command:
```
hadoop fs -text [path_to_the_output_file]
```
Example:
```
hadoop fs -text hadoop/IpRequestBytes/output/part-r-00000.snappy
```
