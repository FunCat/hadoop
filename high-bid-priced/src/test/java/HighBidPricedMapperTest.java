import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class HighBidPricedMapperTest {
    private MapDriver<LongWritable, Text, Text, CityWritable> mapDriver;

    @Before
    public void setUp() {
        HighBidPricedMapper mapper = new HighBidPricedMapper() {
            @Override
            protected void setup(Context context) throws IOException {
                InputStream filePath = HighBidPricedMapperTest.class.getResourceAsStream("/city.en.txt");
                String strLineRead;

                try {
                    brReader = new BufferedReader(new InputStreamReader(filePath));

                    while ((strLineRead = brReader.readLine()) != null) {
                        String deptFieldArray[] = strLineRead.split("\t");
                        cityNames.put(Integer.parseInt(deptFieldArray[0].trim()), deptFieldArray[1].trim());
                    }
                } catch (FileNotFoundException e) {
                    throw new FileNotFoundException("File doesn't exist by the following path: " + filePath);
                } finally {
                    if (brReader != null) {
                        brReader.close();
                    }
                }
            }
        };
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testCase1() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t234\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t277\t48\tnull\t2259\t10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063"));
        mapDriver.withOutput(new Text("zhongshan"), new CityWritable(new IntWritable(234), new IntWritable(277), new Text("Windows NT 5")));
        mapDriver.runTest();
    }

    @Test
    public void testCase2() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("b5d859a10a3a5588204f8b8e35ead2f\t20131019131808811\t1\tD12FlHCie6g\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 6.1; SV1; .NET CLR 2.0.50727)\t113.67.164.*\t216\t217\t3\tdd4270481b753dde29898e27c7c03920\t7d327804a2476a668c633b7fb857fddb\tnull\tEnt_F_Width1\t1000\t90\tNa\tNa\t70\t7336\t294\t255\tnull\t2259\t10048,10057,10067,10059,13496,10077,10093,10075,13042,10102,10006,10024,10148,10031,13776,10111,10127,10063,10116"));
        mapDriver.withOutput(new Text("guangzhou"), new CityWritable(new IntWritable(217), new IntWritable(294), new Text("Windows NT 6")));
        mapDriver.runTest();
    }

    @Test
    public void testCase3() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("a82c799fa5750bc222b8865760ce0228\t20131019140006573\t1\tCBO9rK5JbnL\tMozilla/5.0 (iPad; CPU OS 6_0_1 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A523 Safari/8536.25\t183.235.255.*\t216\t217\t3\t3043163ba84753b4b51dd3290caeae67\t2311c13fac7b397bae9351e3d18324dc\tnull\tNews_F_Rectangle\t300\t250\tNa\tNa\t80\t7323\t294\t80\tnull\t2259\t10057,10048,11278,10067,10059,10684,14273,10075,13042,10083,10006,10024,10149,10110,10123,10031,13776,13403,10063,10116"));
        mapDriver.withOutput(new Text("guangzhou"), new CityWritable(new IntWritable(217), new IntWritable(294), new Text("Mozilla/5.0 (iPad; CPU OS 6_0_1 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A523 Safari/8536.25")));
        mapDriver.runTest();
    }
}