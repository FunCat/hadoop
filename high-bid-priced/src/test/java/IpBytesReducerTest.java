import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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

    private ReduceDriver<Text, CityWritable, Text, CityWritable> reduceDriver;

    @Before
    public void setUp() {
        HighBidPricedReducer reducer = new HighBidPricedReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testCase1() throws IOException {
        Text dongguan = new Text("dongguan");
        Text meizhou = new Text("meizhou");
        Text os = new Text("Windows NT 5");
        CityWritable cityWritable1 = new CityWritable(new IntWritable(233), new IntWritable(294), os);
        CityWritable cityWritable2 = new CityWritable(new IntWritable(233), new IntWritable(354), os);
        CityWritable cityDongguanTotal = new CityWritable(new IntWritable(233), new IntWritable(354), os);
        CityWritable cityWritable3 = new CityWritable(new IntWritable(228), new IntWritable(158), os);
        List<Pair<Text, List<CityWritable>>> values = Arrays.asList(
            new Pair<>(dongguan, Arrays.asList(cityWritable1, cityWritable2)),
            new Pair<>(meizhou, Arrays.asList(cityWritable3))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, CityWritable>> output = Arrays.asList(
            new Pair<>(dongguan, cityDongguanTotal),
            new Pair<>(meizhou, cityWritable3)
        );
        reduceDriver.withAllOutput(output);
        reduceDriver.runTest();
    }

    @Test
    public void testCase2() throws IOException {
        Text ezhou = new Text("ezhou");
        Text fangchenggang = new Text("fangchenggang");
        Text os = new Text("Windows NT 5");
        CityWritable ipWritable1 = new CityWritable(new IntWritable(189), new IntWritable(514), os);
        CityWritable ipWritable2 = new CityWritable(new IntWritable(244), new IntWritable(654), os);
        CityWritable ipWritable2_2 = new CityWritable(new IntWritable(244), new IntWritable(358), os);
        CityWritable ipWritable2_total = new CityWritable(new IntWritable(244), new IntWritable(654), os);

        List<Pair<Text, List<CityWritable>>> values = Arrays.asList(
            new Pair<>(ezhou, Arrays.asList(ipWritable1)),
            new Pair<>(fangchenggang, Arrays.asList(ipWritable2, ipWritable2_2))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, CityWritable>> output = Arrays.asList(
            new Pair<>(ezhou, ipWritable1),
            new Pair<>(fangchenggang, ipWritable2_total)
        );
        reduceDriver.withAllOutput(output);
        reduceDriver.runTest();
    }

    @Test
    public void testCase3() throws IOException {
        Text luzhou = new Text("luzhou");
        Text naqu = new Text("naqu");
        Text os = new Text("Windows NT 5");
        CityWritable ipWritable1 = new CityWritable(new IntWritable(280), new IntWritable(35), os);
        CityWritable ipWritable1_2 = new CityWritable(new IntWritable(280), new IntWritable(10), os);
        CityWritable ipWritable1_total = new CityWritable(new IntWritable(280), new IntWritable(35), os);
        CityWritable ipWritable2 = new CityWritable(new IntWritable(330), new IntWritable(2), os);
        CityWritable ipWritable2_2 = new CityWritable(new IntWritable(330), new IntWritable(59), os);
        CityWritable ipWritable2_total = new CityWritable(new IntWritable(330), new IntWritable(59), os);

        List<Pair<Text, List<CityWritable>>> values = Arrays.asList(
            new Pair<>(luzhou, Arrays.asList(ipWritable1, ipWritable1_2)),
            new Pair<>(naqu, Arrays.asList(ipWritable2, ipWritable2_2))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, CityWritable>> output = Arrays.asList(
            new Pair<>(luzhou, ipWritable1_total),
            new Pair<>(naqu, ipWritable2_total)
        );
        reduceDriver.withAllOutput(output);
        reduceDriver.runTest();
    }
}