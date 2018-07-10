import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class LongestWordReducer extends Reducer<Text, Text, Text, IntWritable> {

    static Logger log = Logger.getLogger(LongestWordReducer.class.getName());
    private int maxLength = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> list = new ArrayList<>();

        for (Text value : values) {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                list.add(tokenizer.nextToken());
            }
        }

        list.stream()
            .max(Comparator.comparing(String::length))
            .ifPresent(s -> maxLength = s.length());
        log.info("REDUCER: The max length: " + maxLength);

        Set<String> setOfLongestWords = list.stream()
            .filter(s -> s.length() == maxLength)
            .collect(Collectors.toSet());

        String longestWords =  setOfLongestWords.stream().
            collect(Collectors.joining(" "));
        log.info("REDUCER: The longest words: " + longestWords);
        System.out.println("Finish");

        context.write(new Text(longestWords), new IntWritable(maxLength));
    }
}
