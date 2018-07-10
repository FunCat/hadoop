import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class LongestWordMapperTest {
    private static final Text LONGEST_WORD = new Text("LONGEST_WORD");
    private MapDriver<LongWritable, Text, Text, Text> mapDriver;

    @Before
    public void setUp() {
        LongestWordMapper mapper = new LongestWordMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testCase1() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("Simple test line"));
        mapDriver.withOutput(LONGEST_WORD, new Text("Simple"));
        mapDriver.runTest();
    }

    @Test
    public void testCase2() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("The Hadoop documentation includes the information you need to get started using Hadoop."));
        mapDriver.withOutput(LONGEST_WORD, new Text("documentation"));
        mapDriver.runTest();
    }

    @Test
    public void testCase3() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("The SecondaryNameNode, JournalNode, and DataNode web UIs have been modernized with HTML5 and Javascript."));
        mapDriver.withOutput(LONGEST_WORD, new Text("SecondaryNameNode"));
        mapDriver.runTest();
    }

    @Test
    public void testInputWithComma() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("comma,"));
        mapDriver.withOutput(LONGEST_WORD, new Text("comma"));
        mapDriver.runTest();
    }

    @Test
    public void testInputWithDot() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("dot."));
        mapDriver.withOutput(LONGEST_WORD, new Text("dot"));
        mapDriver.runTest();
    }

    @Test
    public void testInputWithSlash() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("slash/"));
        mapDriver.withOutput(LONGEST_WORD, new Text("slash"));
        mapDriver.runTest();
    }

    @Test
    public void testInputWithBackSlash() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("backSlash\\"));
        mapDriver.withOutput(LONGEST_WORD, new Text("backSlash"));
        mapDriver.runTest();
    }

    @Test
    public void testInputWithSemicolon() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("semicolon;"));
        mapDriver.withOutput(LONGEST_WORD, new Text("semicolon"));
        mapDriver.runTest();
    }

    @Test
    public void testInputWithColon() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("colon:"));
        mapDriver.withOutput(LONGEST_WORD, new Text("colon"));
        mapDriver.runTest();
    }

    @Test
    public void testInputWithEqualLength() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("one two"));
        mapDriver.withOutput(LONGEST_WORD, new Text("one two"));
        mapDriver.runTest();
    }

    @Test
    public void testInputEmptyLine() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text(""));
        mapDriver.withOutput(LONGEST_WORD, new Text(""));
        mapDriver.runTest();
    }

    @Test
    public void testInputWithNumbers() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("100 test 50000"));
        mapDriver.withOutput(LONGEST_WORD, new Text("50000"));
        mapDriver.runTest();
    }
}