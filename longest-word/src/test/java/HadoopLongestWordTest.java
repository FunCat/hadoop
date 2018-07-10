import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HadoopLongestWordTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, Text, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        LongestWordMapper mapper = new LongestWordMapper();
        LongestWordReducer reducer = new LongestWordReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testCase1() throws IOException {
        List<Pair<LongWritable, Text>> values = Arrays.asList(
            new Pair<>(new LongWritable(0), new Text("Simple test line")),
            new Pair<>(new LongWritable(1), new Text("The second line of the content")),
            new Pair<>(new LongWritable(2), new Text("The last line"))
        );

        mapReduceDriver.withAll(values);
        mapReduceDriver.withOutput(new Text("content"), new IntWritable(7));
        mapReduceDriver.runTest();
    }

    @Test
    public void testCase2() throws IOException {
        List<Pair<LongWritable, Text>> values = Arrays.asList(
            new Pair<>(new LongWritable(0), new Text("Twinkle, twinkle, little star,")),
            new Pair<>(new LongWritable(1), new Text("How I wonder what you are.")),
            new Pair<>(new LongWritable(2), new Text("Up above the world so high,")),
            new Pair<>(new LongWritable(3), new Text("Like a diamond in the sky."))
        );

        mapReduceDriver.withAll(values);
        mapReduceDriver.withOutput(new Text("twinkle diamond Twinkle"), new IntWritable(7));
        mapReduceDriver.runTest();
    }

    @Test
    public void testCase3() throws IOException {
        List<Pair<LongWritable, Text>> values = Arrays.asList(
            new Pair<>(new LongWritable(0), new Text("Every day we see opportunities for business and")),
            new Pair<>(new LongWritable(1), new Text("content creators transformed through the power of the digital economy.")),
            new Pair<>(new LongWritable(2), new Text("We run a number of programmes in the UK to help boost the revenue,")),
            new Pair<>(new LongWritable(3), new Text("reach and productivity of our small business, tech start up and content creator communities,")),
            new Pair<>(new LongWritable(4), new Text("providing practical support and shining a spotlight on their success."))
        );

        mapReduceDriver.withAll(values);
        mapReduceDriver.withOutput(new Text("opportunities"), new IntWritable(13));
        mapReduceDriver.runTest();
    }
}