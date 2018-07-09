import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class LongestWordMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text word = new Text("LONGEST_WORD");
    private Set<String> setOfWords = new HashSet<>();
    private int maxLength = 0;
    private String longestWord = "";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().replaceAll("[^\\w\\s]","");
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            setOfWords.add(tokenizer.nextToken());
        }
        setOfWords.stream()
            .max(Comparator.comparing(String::length))
            .ifPresent(s -> maxLength = LongestWordUtils.updateMaxLength(s));

        setOfWords.stream()
            .filter(s -> s.length() == maxLength)
            .findFirst()
            .ifPresent(s -> longestWord = s);

        context.write(word, new Text(longestWord));
    }
}
