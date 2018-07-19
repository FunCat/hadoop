import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.List;

public class HadoopHighBidPricedTest {

    MapReduceDriver<LongWritable, Text, Text, CityWritable, Text, IntWritable> mapReduceDriver;

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
        HighBidPricedReducer reducer = new HighBidPricedReducer();
        HighBidPricedCombiner combiner = new HighBidPricedCombiner();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer, combiner);
    }

    @Test
    public void differentCityIds() throws IOException {
        List<Pair<LongWritable, Text>> values = Arrays.asList(
            new Pair<>(new LongWritable(0), new Text("bcbc973f1a93e22de83133f360759f04\t20131019134022114\t1\tCALAIF9UcIi\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)\t59.34.170.*\t216\t237\t3\t7ed515fe566938ee6cfbb6ebb7ea4995\tea4e49e1a4b0edabd72386ee533de32f\tnull\tALLINONE_F_Width2\t1000\t90\tNa\tNa\t50\t7336\t294\t50\tnull\t2259\t10059,14273,10117,10075,10083,10102,10006,10148,11423,10110,10031,10126,13403,10063")),
            new Pair<>(new LongWritable(1), new Text("710f5852a9bec40561ea85d7ff51a4e6\t20131019161902429\t1\tCATFtG0VfMY\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0\t183.19.52.*\t216\t226\t2\t13625cb070ffb306b425cd803c4b7ab4\t733d4fa04458005aacd0e3689639fdc5\tnull\t3195670606\t728\t90\tOtherView\tNa\t52\t7330\t277\t52\tnull\t2259\t10057,10048,10059,10079,10076,10077,10083,13866,10006,10024,10148,13776,10111,10063,10116")),
            new Pair<>(new LongWritable(2), new Text("ac0325b62bb7534d3dd7cfa339ca4212\t20131019173901157\t1\tCBTCQD9XeRt\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0)\t119.125.2.*\t216\t228\t1\tb1c53372c546e5032d336c35a5df6bea\t63cff11fcd610af7a2e8fe96161e4478\tnull\tmm_34061135_3437851_13062004\t300\t250\tNa\tNa\t0\t7323\t294\t239\tnull\t2259\t10048,10057,10067,10059,10079,10076,10075,10083,10024,10006,10148,10110,13776,16706,10031,10052,13403,10133,10063,10116,10125"))
        );
        mapReduceDriver.withAll(values);
        List<Pair<Text, IntWritable>> output = Arrays.asList(
            new Pair<>(new Text("meizhou"), new IntWritable(1)),
            new Pair<>(new Text("yunfu"), new IntWritable(1)),
            new Pair<>(new Text("zhaoqing"), new IntWritable(1))
        );
        mapReduceDriver.withAllOutput(output);
        mapReduceDriver.runTest();
    }

    @Test
    public void identicalCityId() throws IOException {
        List<Pair<LongWritable, Text>> values = Arrays.asList(
            new Pair<>(new LongWritable(0), new Text("ac0325b62bb7534d3dd7cfa339ca4212\t20131019173901157\t1\tCBTCQD9XeRt\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0)\t119.125.2.*\t216\t228\t1\tb1c53372c546e5032d336c35a5df6bea\t63cff11fcd610af7a2e8fe96161e4478\tnull\tmm_34061135_3437851_13062004\t300\t250\tNa\tNa\t0\t7323\t294\t239\tnull\t2259\t10048,10057,10067,10059,10079,10076,10075,10083,10024,10006,10148,10110,13776,16706,10031,10052,13403,10133,10063,10116,10125")),
            new Pair<>(new LongWritable(1), new Text("557d55d7d4f14a88757e892cfbd6ea6e\t20131019012500685\t1\tCBPAv4E1eQI\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; SE 2.X MetaSr 1.0)\t183.37.82.*\t216\t219\t2\t76af2d69f93c42488a768ef07e564c5\tf21cb3e2e834286977259a42eec3c06b\tnull\t3534628899\t468\t60\tOtherView\tNa\t5\t7328\t277\t14\tnull\t2259\t10067,10059,10684,14273,10076,10083,13042,10006,13866,10110,10031,10052,10133,10063,10116")),
            new Pair<>(new LongWritable(2), new Text("90af02cea34a1b548715dbb01d9cbc20\t20131019131419018\t1\tCCLDDq3kdTH\tMozilla/5.0(iPad; U;CPU OS 6_1 like Mac OS X; zh-CN; iPad3,3) AppleWebKit/534.46 (KHTML, like Gecko) UCBrowser/2.0.1.280 U3/0.8.0 Safari/7543.48.3\t113.116.116.*\t216\t219\t3\t3043163ba84753b4b51dd3290caeae67\tbfa000ed663f4997db218d6da6b1510f\tnull\tNews_Width4\t1000\t90\tNa\tNa\t50\t7336\t294\t50\tnull\t2259\t10048,11278,10067,16661,11379,14273,10083,13042,11423,10110,13776,11512,13403,10133,10063,10116,11092,10057,11724,13800,16593,16617,10059,10684,10079,10076,10077,10075,10093,13866,10024,10006,10149,16706,10031,10052,11680,10125"))
        );
        mapReduceDriver.withAll(values);
        List<Pair<Text, IntWritable>> output = Arrays.asList(
            new Pair<>(new Text("meizhou"), new IntWritable(1)),
            new Pair<>(new Text("shenzhen"), new IntWritable(2))
        );
        mapReduceDriver.withAllOutput(output);
        mapReduceDriver.runTest();
    }

    @Test
    public void testCaseWhenLessThan250() throws IOException {
        List<Pair<LongWritable, Text>> values = Arrays.asList(
            new Pair<>(new LongWritable(0), new Text("ac0325b62bb7534d3dd7cfa339ca4212\t20131019173901157\t1\tCBTCQD9XeRt\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0)\t119.125.2.*\t216\t228\t1\tb1c53372c546e5032d336c35a5df6bea\t63cff11fcd610af7a2e8fe96161e4478\tnull\tmm_34061135_3437851_13062004\t300\t250\tNa\tNa\t0\t7323\t294\t239\tnull\t2259\t10048,10057,10067,10059,10079,10076,10075,10083,10024,10006,10148,10110,13776,16706,10031,10052,13403,10133,10063,10116,10125")),
            new Pair<>(new LongWritable(1), new Text("557d55d7d4f14a88757e892cfbd6ea6e\t20131019012500685\t1\tCBPAv4E1eQI\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; SE 2.X MetaSr 1.0)\t183.37.82.*\t216\t219\t2\t76af2d69f93c42488a768ef07e564c5\tf21cb3e2e834286977259a42eec3c06b\tnull\t3534628899\t468\t60\tOtherView\tNa\t5\t7328\t277\t14\tnull\t2259\t10067,10059,10684,14273,10076,10083,13042,10006,13866,10110,10031,10052,10133,10063,10116")),
            new Pair<>(new LongWritable(2), new Text("90af02cea34a1b548715dbb01d9cbc20\t20131019131419018\t1\tCCLDDq3kdTH\tMozilla/5.0(iPad; U;CPU OS 6_1 like Mac OS X; zh-CN; iPad3,3) AppleWebKit/534.46 (KHTML, like Gecko) UCBrowser/2.0.1.280 U3/0.8.0 Safari/7543.48.3\t113.116.116.*\t216\t219\t3\t3043163ba84753b4b51dd3290caeae67\tbfa000ed663f4997db218d6da6b1510f\tnull\tNews_Width4\t1000\t90\tNa\tNa\t50\t7336\t234\t50\tnull\t2259\t10048,11278,10067,16661,11379,14273,10083,13042,11423,10110,13776,11512,13403,10133,10063,10116,11092,10057,11724,13800,16593,16617,10059,10684,10079,10076,10077,10075,10093,13866,10024,10006,10149,16706,10031,10052,11680,10125"))
        );
        mapReduceDriver.withAll(values);
        List<Pair<Text, IntWritable>> output = Arrays.asList(
            new Pair<>(new Text("meizhou"), new IntWritable(1)),
            new Pair<>(new Text("shenzhen"), new IntWritable(1))
        );
        mapReduceDriver.withAllOutput(output);
        mapReduceDriver.runTest();
    }

}