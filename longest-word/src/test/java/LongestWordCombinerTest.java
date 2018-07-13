import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LongestWordCombinerTest {

    private static final Text LONGEST_WORD = new Text("LONGEST_WORD");
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        LongestWordCombiner reducer = new LongestWordCombiner();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testCase1() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("one"));
        values.add(new Text("two"));
        values.add(new Text("three"));
        reduceDriver.withInput(LONGEST_WORD, values);
        reduceDriver.withOutput(LONGEST_WORD, new Text("one two three"));
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
        reduceDriver.withOutput(LONGEST_WORD, new Text("hive kafka hadoop pig"));
        reduceDriver.runTest();
    }

    @Test
    public void testCase3() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("cat"));
        values.add(new Text("rabbit"));
        values.add(new Text("mouse"));
        values.add(new Text("duck"));
        values.add(new Text("dog"));
        reduceDriver.withInput(LONGEST_WORD, values);
        reduceDriver.withOutput(LONGEST_WORD, new Text("mouse duck cat rabbit dog"));
        reduceDriver.runTest();
    }

    @Test
    public void testInputWithEqualLength() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("car"));
        values.add(new Text("bus"));
        reduceDriver.withInput(LONGEST_WORD, values);
        reduceDriver.withOutput(LONGEST_WORD, new Text("bus car"));
        reduceDriver.runTest();
    }

    @Test
    public void testInputWithDuplicates() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("car"));
        values.add(new Text("car"));
        reduceDriver.withInput(LONGEST_WORD, values);
        reduceDriver.withOutput(LONGEST_WORD, new Text("car"));
        reduceDriver.runTest();
    }
}