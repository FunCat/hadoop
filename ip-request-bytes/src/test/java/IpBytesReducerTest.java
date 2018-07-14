import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class IpBytesReducerTest {

    private ReduceDriver<Text, IpWritable, Text, IpWritable> reduceDriver;

    @Before
    public void setUp() {
        IpBytesReducer reducer = new IpBytesReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testCase1() throws IOException {
        Text ip1 = new Text("ip1");
        Text ip2 = new Text("ip2");
        IpWritable ipWritable1 = new IpWritable(new FloatWritable(10), new LongWritable(10));
        IpWritable ipWritable2 = new IpWritable(new FloatWritable(20), new LongWritable(20));
        List<Pair<Text, List<IpWritable>>> values = Arrays.asList(
            new Pair<>(ip1, Arrays.asList(ipWritable1)),
            new Pair<>(ip2, Arrays.asList(ipWritable2))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, IpWritable>> output = Arrays.asList(
            new Pair<>(ip1, ipWritable1),
            new Pair<>(ip2, ipWritable2)
        );
        reduceDriver.withAllOutput(output);
        reduceDriver.runTest();
    }

    @Test
    public void testCase2() throws IOException {
        Text ip1 = new Text("ip1");
        Text ip2 = new Text("ip2");
        IpWritable ipWritable1 = new IpWritable(new FloatWritable(514), new LongWritable(514));
        IpWritable ipWritable1_total = new IpWritable(new FloatWritable(514), new LongWritable(514));
        IpWritable ipWritable2 = new IpWritable(new FloatWritable(654), new LongWritable(654));
        IpWritable ipWritable2_2 = new IpWritable(new FloatWritable(358), new LongWritable(358));
        IpWritable ipWritable2_total = new IpWritable(new FloatWritable(506), new LongWritable(1012));

        List<Pair<Text, List<IpWritable>>> values = Arrays.asList(
            new Pair<>(ip1, Arrays.asList(ipWritable1)),
            new Pair<>(ip2, Arrays.asList(ipWritable2, ipWritable2_2))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, IpWritable>> output = Arrays.asList(
            new Pair<>(ip1, ipWritable1_total),
            new Pair<>(ip2, ipWritable2_total)
        );
        reduceDriver.withAllOutput(output);
        reduceDriver.runTest();
    }

    @Test
    public void testCase3() throws IOException {
        Text ip1 = new Text("ip1");
        Text ip2 = new Text("ip2");
        IpWritable ipWritable1 = new IpWritable(new FloatWritable(35), new LongWritable(35));
        IpWritable ipWritable1_2 = new IpWritable(new FloatWritable(10), new LongWritable(10));
        IpWritable ipWritable1_total = new IpWritable(new FloatWritable(22.5f), new LongWritable(45));
        IpWritable ipWritable2 = new IpWritable(new FloatWritable(2), new LongWritable(2));
        IpWritable ipWritable2_2 = new IpWritable(new FloatWritable(59), new LongWritable(59));
        IpWritable ipWritable2_total = new IpWritable(new FloatWritable(30.5f), new LongWritable(61));

        List<Pair<Text, List<IpWritable>>> values = Arrays.asList(
            new Pair<>(ip1, Arrays.asList(ipWritable1, ipWritable1_2)),
            new Pair<>(ip2, Arrays.asList(ipWritable2, ipWritable2_2))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, IpWritable>> output = Arrays.asList(
            new Pair<>(ip1, ipWritable1_total),
            new Pair<>(ip2, ipWritable2_total)
        );
        reduceDriver.withAllOutput(output);
        reduceDriver.runTest();
    }

    @Test
    public void testCase4() throws IOException {
        Text ip1 = new Text("ip1");
        Text ip2 = new Text("ip2");
        IpWritable ipWritable1 = new IpWritable(new FloatWritable(10.5f), new LongWritable(35));
        IpWritable ipWritable1_2 = new IpWritable(new FloatWritable(48.0f), new LongWritable(94));
        IpWritable ipWritable1_total = new IpWritable(new FloatWritable(29.25f), new LongWritable(129));
        IpWritable ipWritable2 = new IpWritable(new FloatWritable(84.5f), new LongWritable(105));
        IpWritable ipWritable2_2 = new IpWritable(new FloatWritable(37.25f), new LongWritable(48));
        IpWritable ipWritable2_total = new IpWritable(new FloatWritable(60.875f), new LongWritable(153));

        List<Pair<Text, List<IpWritable>>> values = Arrays.asList(
            new Pair<>(ip1, Arrays.asList(ipWritable1, ipWritable1_2)),
            new Pair<>(ip2, Arrays.asList(ipWritable2, ipWritable2_2))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, IpWritable>> output = Arrays.asList(
            new Pair<>(ip1, ipWritable1_total),
            new Pair<>(ip2, ipWritable2_total)
        );
        reduceDriver.withAllOutput(output);
        reduceDriver.runTest();
    }
}