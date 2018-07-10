import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class LongestWordMapper extends Mapper<LongWritable, Text, Text, Text> {

    static Logger log = Logger.getLogger(LongestWordMapper.class.getName());
    private Text word = new Text("LONGEST_WORD");
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
        setOfWords.stream()
            .max(Comparator.comparing(String::length))
            .ifPresent(s -> maxLength = s.length());
        log.info("MAPPER: The max length: " + maxLength);

        String longestWords = setOfWords.stream()
            .filter(s -> s.length() == maxLength)
            .collect(Collectors.joining(" "));

        log.info("MAPPER: The longest words: " + longestWords);
        context.write(word, new Text(longestWords));
    }
}
