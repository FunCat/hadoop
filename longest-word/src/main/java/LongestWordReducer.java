import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class LongestWordReducer extends Reducer<Text, Text, Text, IntWritable> {

    private int maxLength = 0;
    private String longestWord = "";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Text> list = new ArrayList<>();
        CollectionUtils.addAll(list, values.iterator());

        list.stream()
            .max(Comparator.comparing(s -> s.toString().length()))
            .ifPresent(s -> maxLength = LongestWordUtils.updateMaxLength(s));

        list.stream()
            .filter(s -> s.toString().length() == maxLength)
            .findFirst()
            .ifPresent(s -> longestWord = s.toString());

        context.write(new Text(longestWord), new IntWritable(longestWord.length()));
    }
}
