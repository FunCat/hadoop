import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HighBidPricedReducerTest {

    private ReduceDriver<Text, CityWritable, Text, IntWritable> reduceDriver;

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
        CityWritable cityWritable3 = new CityWritable(new IntWritable(228), new IntWritable(283), os);
        List<Pair<Text, List<CityWritable>>> values = Arrays.asList(
            new Pair<>(dongguan, Arrays.asList(cityWritable1, cityWritable2)),
            new Pair<>(meizhou, Arrays.asList(cityWritable3))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, IntWritable>> output = Arrays.asList(
            new Pair<>(dongguan, new IntWritable(648)),
            new Pair<>(meizhou, new IntWritable(283))
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

        List<Pair<Text, List<CityWritable>>> values = Arrays.asList(
            new Pair<>(ezhou, Arrays.asList(ipWritable1)),
            new Pair<>(fangchenggang, Arrays.asList(ipWritable2, ipWritable2_2))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, IntWritable>> output = Arrays.asList(
            new Pair<>(ezhou, new IntWritable(514)),
            new Pair<>(fangchenggang, new IntWritable(1012))
        );
        reduceDriver.withAllOutput(output);
        reduceDriver.runTest();
    }

    @Test
    public void testCase3() throws IOException {
        Text luzhou = new Text("luzhou");
        Text naqu = new Text("naqu");
        Text os = new Text("Windows NT 5");
        CityWritable ipWritable1 = new CityWritable(new IntWritable(280), new IntWritable(254), os);
        CityWritable ipWritable1_2 = new CityWritable(new IntWritable(280), new IntWritable(294), os);
        CityWritable ipWritable2 = new CityWritable(new IntWritable(330), new IntWritable(2), os);
        CityWritable ipWritable2_2 = new CityWritable(new IntWritable(330), new IntWritable(277), os);

        List<Pair<Text, List<CityWritable>>> values = Arrays.asList(
            new Pair<>(luzhou, Arrays.asList(ipWritable1, ipWritable1_2)),
            new Pair<>(naqu, Arrays.asList(ipWritable2, ipWritable2_2))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, IntWritable>> output = Arrays.asList(
            new Pair<>(luzhou, new IntWritable(548)),
            new Pair<>(naqu, new IntWritable(279))
        );
        reduceDriver.withAllOutput(output);
        reduceDriver.runTest();
    }
}