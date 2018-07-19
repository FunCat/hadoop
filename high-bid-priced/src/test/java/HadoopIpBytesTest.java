import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HadoopIpBytesTest {

//    MapReduceDriver<LongWritable, Text, Text, IpWritable, Text, IpWritable> mapReduceDriver;
//
//    @Before
//    public void setUp() {
//        IpBytesMapper mapper = new IpBytesMapper();
//        IpBytesReducer reducer = new IpBytesReducer();
//        IpBytesCombiner combiner = new IpBytesCombiner();
//        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer, combiner);
//    }
//
//    @Test
//    public void testCase1() throws IOException {
//        List<Pair<LongWritable, Text>> values = Arrays.asList(
//            new Pair<>(new LongWritable(0), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"")),
//            new Pair<>(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"")),
//            new Pair<>(new LongWritable(2), new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/pdf.gif HTTP/1.1\" 200 390 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\""))
//        );
//        mapReduceDriver.withAll(values);
//        List<Pair<Text, IpWritable>> output = Arrays.asList(
//            new Pair<>(new Text("ip1"), new IpWritable(new FloatWritable(48478), new LongWritable(96956))),
//            new Pair<>(new Text("ip2"), new IpWritable(new FloatWritable(390), new LongWritable(390)))
//        );
//        mapReduceDriver.withAllOutput(output);
//        mapReduceDriver.runTest();
//    }
//
//    @Test
//    public void testCase2() throws IOException {
//        List<Pair<LongWritable, Text>> values = Arrays.asList(
//            new Pair<>(new LongWritable(0), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"")),
//            new Pair<>(new LongWritable(1), new Text("ip3 - - [24/Apr/2011:04:22:45 -0400] \"GET /personal/vanagon_1.jpg HTTP/1.1\" 200 72209 \"http://www.inetgiant.in/addetails/1985-vw-vanagon-transporter-single-cab-sinka-diesel-truck-2wd-rhd/3235819\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; WOW64; Trident/4.0; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C)\"")),
//            new Pair<>(new LongWritable(2), new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/pdf.gif HTTP/1.1\" 200 390 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\""))
//        );
//        mapReduceDriver.withAll(values);
//        List<Pair<Text, IpWritable>> output = Arrays.asList(
//            new Pair<>(new Text("ip1"), new IpWritable(new FloatWritable(40028), new LongWritable(40028))),
//            new Pair<>(new Text("ip2"), new IpWritable(new FloatWritable(390), new LongWritable(390))),
//            new Pair<>(new Text("ip3"), new IpWritable(new FloatWritable(72209), new LongWritable(72209)))
//        );
//        mapReduceDriver.withAllOutput(output);
//        mapReduceDriver.runTest();
//    }
//
//    @Test
//    public void testCase3() throws IOException {
//        List<Pair<LongWritable, Text>> values = Arrays.asList(
//            new Pair<>(new LongWritable(0), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""))
//        );
//        mapReduceDriver.withAll(values);
//        List<Pair<Text, IpWritable>> output = Arrays.asList(
//            new Pair<>(new Text("ip1"), new IpWritable(new FloatWritable(40028), new LongWritable(40028)))
//        );
//        mapReduceDriver.withAllOutput(output);
//        mapReduceDriver.runTest();
//    }

}