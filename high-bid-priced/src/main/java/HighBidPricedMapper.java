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

            bufferCity.getCity(cityId, biddingPrice, os);
            if(cityNames.containsKey(cityId)) {
                buffer.set(cityNames.get(cityId));
            }
            else{
                buffer.set(cityNames.get(0));
            }
            context.write(buffer, bufferCity);
        } else {
            throw new InvalidInputException(Collections.singletonList(new IOException("Invalid input format! " + line)));
        }
    }
}
