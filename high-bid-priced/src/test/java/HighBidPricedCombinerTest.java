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

public class HighBidPricedCombinerTest {

    private FloatWritable avgBytes = new FloatWritable(0);
    private ReduceDriver<Text, CityWritable, Text, CityWritable> reduceDriver;

    @Before
    public void setUp() {
        HighBidPricedCombiner reducer = new HighBidPricedCombiner();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void differentCityIds() throws IOException {
        Text shaoguan = new Text("shaoguan");
        Text qinzhou = new Text("qinzhou");
        Text os = new Text("Windows NT 4");
        CityWritable cityWritable = new CityWritable(new IntWritable(218), new IntWritable(294), os);
        CityWritable cityWritable1_total = new CityWritable(new IntWritable(218), new IntWritable(1), os);
        CityWritable cityWritable2 = new CityWritable(new IntWritable(245), new IntWritable(277), os);
        CityWritable cityWritable2_total = new CityWritable(new IntWritable(245), new IntWritable(1), os);
        List<Pair<Text, List<CityWritable>>> values = Arrays.asList(
            new Pair<>(shaoguan, Arrays.asList(cityWritable)),
            new Pair<>(qinzhou, Arrays.asList(cityWritable2))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, CityWritable>> output = Arrays.asList(
            new Pair<>(shaoguan, cityWritable1_total),
            new Pair<>(qinzhou, cityWritable2_total)
        );
        reduceDriver.withAllOutput(output);
        reduceDriver.runTest();
    }

    @Test
    public void identicalCityId() throws IOException {
        Text shaoguan = new Text("shaoguan");
        Text os = new Text("Windows NT 4");
        CityWritable cityWritable = new CityWritable(new IntWritable(218), new IntWritable(294), os);
        CityWritable cityWritable2 = new CityWritable(new IntWritable(218), new IntWritable(277), os);
        CityWritable cityWritable1_total = new CityWritable(new IntWritable(218), new IntWritable(2), os);

        List<Pair<Text, List<CityWritable>>> values = Arrays.asList(
            new Pair<>(shaoguan, Arrays.asList(cityWritable, cityWritable2))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, CityWritable>> output = Arrays.asList(
            new Pair<>(shaoguan, cityWritable1_total)
        );
        reduceDriver.withAllOutput(output);
        reduceDriver.runTest();
    }

    @Test
    public void testCaseWhenLessThan250() throws IOException {
        Text shaoguan = new Text("shaoguan");
        Text os = new Text("Windows NT 4");
        CityWritable cityWritable = new CityWritable(new IntWritable(218), new IntWritable(294), os);
        CityWritable cityWritable2 = new CityWritable(new IntWritable(218), new IntWritable(234), os);
        CityWritable cityWritable1_total = new CityWritable(new IntWritable(218), new IntWritable(1), os);

        List<Pair<Text, List<CityWritable>>> values = Arrays.asList(
            new Pair<>(shaoguan, Arrays.asList(cityWritable, cityWritable2))
        );
        reduceDriver.withAll(values);
        List<Pair<Text, CityWritable>> output = Arrays.asList(
            new Pair<>(shaoguan, cityWritable1_total)
        );
        reduceDriver.withAllOutput(output);
        reduceDriver.runTest();
    }
}