import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * LongestWordReducer find the longest word and returns the word with its length.
 * If Reducer finds some words with the same length, the Reducer will return a string of these words
 * joined by space.
 *
 *  For example:
 *
 *      INPUT                  OUTPUT
 *  one two three       ->  three 5
 *  hadoop kafka pig    ->  hadoop 6
 *  cat rabbit mouse    ->  rabbit 6
 *  car bus             ->  bus car 3
 *
 */
public class LongestWordReducer extends Reducer<Text, Text, Text, IntWritable> {

    private static Logger log = Logger.getLogger(LongestWordReducer.class.getName());
    private Text bufferText = new Text();
    private IntWritable bufferInt = new IntWritable();
    private int maxLength = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> words = new HashSet<>();

        for (Text value : values) {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                words.add(tokenizer.nextToken());
            }
        }

        words.stream()
            .max(Comparator.comparing(String::length))
            .ifPresent(s -> maxLength = s.length());
        log.info("REDUCER: The max length: " + maxLength);

        Set<String> setOfLongestWords = words.stream()
            .filter(s -> s.length() == maxLength)
            .collect(Collectors.toSet());

        String longestWords =  setOfLongestWords.stream().
            collect(Collectors.joining(" "));
        log.info("REDUCER: The longest words: " + longestWords);

        bufferText.set(longestWords);
        bufferInt.set(maxLength);

        context.write(bufferText, bufferInt);
    }
}
