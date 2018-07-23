import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * LongestWordCombiner get the all longest words by each line from {@link LongestWordMapper}.
 * The Combiner filtered all words and remove all duplications, because we can have a situation when
 * we get the same longest word from the first and the third lines. So, in this case we want to get
 * only one copy of this word, but not two.
 * Does not provide the original word order. The output order is provided by the hash of each word.
 *
 *  For example:
 *
 *      INPUT                  OUTPUT
 *  one two three       ->  one two three
 *  hadoop kafka pig    ->  kafka hadoop pig
 *  cat rabbit mouse    ->  mouse cat rabbit
 *  car bus             ->  bus car
 *  car car             ->  car
 *
 */
public class LongestWordCombiner extends Reducer<Text, Text, Text, Text> {

    private static Logger log = Logger.getLogger(LongestWordReducer.class.getName());
    private Text word = new Text("LONGEST_WORD");
    private Text buffer = new Text();
    private int maxLength = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> words = new HashSet<>();

        for (Text value : values) {
            words.add(value.toString());
        }

        Optional<String> max = words.stream()
            .max(Comparator.comparing(String::length));

        if(max.isPresent()){
            int localMaxLength = max.get().length();
            if(localMaxLength >= maxLength) {
                maxLength = localMaxLength;
                log.info("COMBINER: The max length: " + maxLength);

                List<String> longestWords = words.stream()
                    .filter(s -> s.length() == maxLength)
                    .collect(Collectors.toList());
                log.info("COMBINER: The longest words: " + longestWords);

                for (String longestWord : longestWords) {
                    buffer.set(longestWord);
                    context.write(word, buffer);
                }
            }
        }
    }
}
