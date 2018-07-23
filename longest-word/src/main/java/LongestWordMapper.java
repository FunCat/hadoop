import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * LongestWordMapper reads the file from hdfs line by line, splits each line on tokens and after that
 * return the longest word for each line.
 * If we have some words with the same length, the Mapper will return a string of these words joined
 * by space.
 * If the line consists the punctuation symbols like, they will be removed from the line.
 * Does not provide the original word order. The output order is provided by the hash of each word.
 *
 * In the LongestWordMapper the custom word 'LONGEST_WORD' was used to specify the same key for each
 * values. This allows us send all our words to the one {@link LongestWordCombiner}.
 *
 * For example:
 *
 *      INPUT                OUTPUT
 *  The first line      ->  first
 *  Next line           ->  line Next
 *  To be continued     ->  continued
 *  Comma, in the line  ->  Comma
 *  50000 test text     ->  50000
 *
 */
public class LongestWordMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Logger log = Logger.getLogger(LongestWordMapper.class.getName());
    private Text word = new Text("LONGEST_WORD");
    private Text buffer = new Text();
    private int maxLength = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Set<String> setOfWords = new HashSet<>();

        String line = value.toString().replaceAll("[^\\w\\s]","");
        log.info("MAPPER: Processing the line: " + line);

        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            setOfWords.add(tokenizer.nextToken());
        }

        Optional<String> max = setOfWords.stream()
            .max(Comparator.comparing(String::length));

        if(max.isPresent()){
            int localMaxLength = max.get().length();
            if(localMaxLength >= maxLength) {
                maxLength = localMaxLength;
                log.info("MAPPER: The max length: " + maxLength);

                List<String> longestWords = setOfWords.stream()
                    .filter(s -> s.length() == maxLength)
                    .collect(Collectors.toList());
                log.info("MAPPER: The longest words: " + longestWords);

                for (String longestWord : longestWords) {
                    buffer.set(longestWord);
                    context.write(word, buffer);
                }
            }
        }
    }
}
