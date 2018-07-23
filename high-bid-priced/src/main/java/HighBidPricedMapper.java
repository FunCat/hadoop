import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The HighBidPricedMapper reads the file from hdfs line by line, uses the regexp to get the useful information
 * and create {@link CityWritable} with this information.
 *
 * The HighBidPricedMapper uses the counter field not as a counter. The mapper save the bidding price
 * to this field and the {@link HighBidPricedCombiner} will calculate how many cities have the
 * bidding price higher than 250.
 *
 * For example:
 * INPUT:
 * 2e72d1bd7185fb76d69c852c57436d37	20131019025500549	1	CAD06D3WCtf	Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)	113.117.187.*	216	234	2	33235ca84c5fee9254e6512a41b3ad5e	8bbb5a81cc3d680dd0c27cf4886ddeae	null	3061584349	728	90	OtherView	Na	5	7330	277	48	null	2259	10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063
 * OUTPUT:
 * cityId - 234
 * counter - 277
 * os - Windows NT 5
 */
public class HighBidPricedMapper extends Mapper<LongWritable, Text, Text, CityWritable> {

    private static Logger log = Logger.getLogger(HighBidPricedMapper.class.getName());
    private Text buffer = new Text();
    private CityWritable bufferCity = new CityWritable();
    private static Pattern patternWindows = Pattern.compile("[\\w]*\t[\\d]*\t[\\d]*\t[\\w\\W]*\t[\\w\\S\\d\\s]*(Windows NT [\\d]*)[.\\d]*[\\w\\S\\d\\s]*\t[\\d]*[.][\\d]*[.][\\d]*[.][*]\t[\\d]*\t([\\d]*)\t[\\d\\w]*\t[\\w]*\t[\\w]*\t[\\w]*\t[\\w]*\t[\\d]*\t[\\d]*\t[\\w]*\t[\\w]*\t[\\d]*\t[\\d]*\t([\\d]*)\t[\\d]*\t[\\w]*\t[\\d]*\t[\\d,\\w]*");
    private static Pattern patternOthers = Pattern.compile("[\\w]*\t[\\d]*\t[\\d]*\t[\\w\\W]*\t([\\w\\S\\d\\s]*)\t[\\d]*[.][\\d]*[.][\\d]*[.][*]\t[\\d]*\t([\\d]*)\t[\\d\\w]*\t[\\w]*\t[\\w]*\t[\\w]*\t[\\w]*\t[\\d]*\t[\\d]*\t[\\w]*\t[\\w]*\t[\\d]*\t[\\d]*\t([\\d]*)\t[\\d]*\t[\\w]*\t[\\d]*\t[\\d,\\w]*");

    static HashMap<Integer, String> cityNames = new HashMap<>();
    BufferedReader brReader;

    @Override
    protected void setup(Context context) throws IOException {

        Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());

        for (Path filePath : cacheFilesLocal) {
            String strLineRead;

            try {
                brReader = new BufferedReader(new FileReader(filePath.toString()));

                while ((strLineRead = brReader.readLine()) != null) {
                    String deptFieldArray[] = strLineRead.split("\t");
                    cityNames.put(Integer.parseInt(deptFieldArray[0].trim()), deptFieldArray[1].trim());
                }
            } catch (FileNotFoundException e) {
                throw new FileNotFoundException("File doesn't exist by the following path: " + filePath);
            }finally {
                if (brReader != null) {
                    brReader.close();
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        Matcher matcher;
        if(line.contains("Windows NT")) {
            matcher = patternWindows.matcher(line);
        }
        else {
            matcher = patternOthers.matcher(line);
        }
        log.info("MAPPER: Processing the line: " + line);

        if (matcher.matches()) {
            String os = matcher.group(1);
            int cityId = Integer.parseInt(matcher.group(2));
            int biddingPrice = Integer.parseInt(matcher.group(3));

            if (biddingPrice > 250) {
                bufferCity.getCity(cityId, biddingPrice, os);
                if (cityNames.containsKey(cityId)) {
                    buffer.set(cityNames.get(cityId));
                } else {
                    buffer.set(cityNames.get(0));
                }
                context.write(buffer, bufferCity);
            }
        } else {
            throw new InvalidInputException(Collections.singletonList(new IOException("Invalid input format! " + line)));
        }
    }
}
