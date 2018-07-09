import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class LongestWordReducerTest {

    private static final Text LONGEST_WORD = new Text("LONGEST_WORD");
    private ReduceDriver<Text, Text, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        LongestWordReducer reducer = new LongestWordReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testCase1() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("one"));
        values.add(new Text("two"));
        values.add(new Text("three"));
        reduceDriver.withInput(LONGEST_WORD, values);
        reduceDriver.withOutput(new Text("three"), new IntWritable(5));
        reduceDriver.runTest();
    }

    @Test
    public void testCase2() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("hadoop"));
        values.add(new Text("kafka"));
        values.add(new Text("pig"));
        values.add(new Text("hive"));
        reduceDriver.withInput(LONGEST_WORD, values);
        reduceDriver.withOutput(new Text("hadoop"), new IntWritable(6));
        reduceDriver.runTest();
    }

}