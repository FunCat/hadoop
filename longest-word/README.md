# Longest word
The HadoopLongestWord application find the longest word in th input file and return the longest
word with its length. It can be a several words, if they have the same longest length.

For running application you need to compile it by the following command:
```gradle :longest-word:clean :longest-word:build```

After that you need to run the application on the hadoop environment. Before the start put
the input file to the hdfs and check that the directory for the output doesn't exist.
Run the following command:
```hadoop jar [path_to_the_jar] [path_to_the_input_file] [path_to_the_output_directory]```
Example:
```hadoop jar longest-word-1.0-SNAPSHOT.jar /input.txt /output```