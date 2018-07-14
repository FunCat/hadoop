import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class IpBytesMapperTest {
    private MapDriver<LongWritable, Text, Text, IpWritable> mapDriver;

    @Before
    public void setUp() {
        IpBytesMapper mapper = new IpBytesMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testCase1() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapDriver.withOutput(new Text("ip1"), new IpWritable(new FloatWritable(0), new LongWritable(40028)));
        mapDriver.runTest();
    }

    @Test
    public void testCase2() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/pdf.gif HTTP/1.1\" 200 390 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\""));
        mapDriver.withOutput(new Text("ip2"), new IpWritable(new FloatWritable(0), new LongWritable(390)));
        mapDriver.runTest();
    }

    @Test
    public void testCase3() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("ip6 - - [24/Apr/2011:04:25:26 -0400] \"GET / HTTP/1.1\" 200 12550 \"-\" \"Baiduspider+(+http://www.baidu.com/search/spider.htm)\""));
        mapDriver.withOutput(new Text("ip6"), new IpWritable(new FloatWritable(0), new LongWritable(12550)));
        mapDriver.runTest();
    }

    @Test(expected = InvalidInputException.class)
    public void testInvalidInput() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("ip6"));
        mapDriver.run();
    }
}