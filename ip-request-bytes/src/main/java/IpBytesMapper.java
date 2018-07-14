import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Collections;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * IpBytesMapper reads the file from hdfs line by line, uses the regexp to get the useful information
 * and create {@link IpWritable} with this information.
 * The IpBytesMapper doesn't calculate the average number of bytes. It is only create {@link IpWritable}
 * entities with the information about the bytes and after that send the information to the {@link IpBytesCombiner}.
 *
 * Also, IpBytesMapper uses the counters to calculate how many requests were from each type of browsers.
 *
 * For example:
 * INPUT:
 * ip1 - - [24/Apr/2011:04:06:01 -0400] "GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1" 200 40028 "-" "Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)"
 * OUTPUT:
 * ip - ip1
 * avgBytes - 0
 * totalBytes - 40028
 */
public class IpBytesMapper extends Mapper<LongWritable, Text, Text, IpWritable> {

    private static Logger log = Logger.getLogger(IpBytesMapper.class.getName());
    private Text buffer = new Text();
    private IpWritable bufferIp = new IpWritable();
    private static Pattern patternOkRequest = Pattern.compile("(ip\\d+) (\\W)* \\[.*\\] \"([\\s\\w\\S]+?)\" [\\d]* ([\\d]*) \"([\\s\\w\\S]*?)\" \"((\\w*\\/[\\d.]*)*[\\s\\w\\S]+?)\"");
    private static Pattern patternBadRequest = Pattern.compile("(ip\\d+) (\\W)* \\[.*\\] \"([\\s\\w\\S]+?)\" [\\d]* - \"([\\s\\w\\S]+?)\" \"((\\w*\\/[\\d.]*)*[\\s\\w\\S]+?)\"");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        Matcher matcher = patternOkRequest.matcher(line);
        log.info("MAPPER: Processing the line: " + line);

        if (matcher.matches() && matcher.groupCount() == 7) {
            String ip = matcher.group(1);
            String bytes = matcher.group(4);
            String userAgent = matcher.group(6);
            String browser = matcher.group(7);

            if (browser != null) {
                incCounter(context, matcher.group(7));
            }
            log.info("MAPPER: ip: " + ip + ", bytes: " + bytes + ", user agent: " + userAgent);

            bufferIp.getIp(0, bytes);
            buffer.set(ip);
            context.write(buffer, bufferIp);
        } else {
            matcher = patternBadRequest.matcher(line);
            if (matcher.matches() && matcher.groupCount() == 6) {
                String ip = matcher.group(1);
                String userAgent = matcher.group(5);
                String browser = matcher.group(6);

                if (browser != null) {
                    incCounter(context, browser);
                }
                log.info("MAPPER: ip: " + ip + ", bytes: 0, user agent: " + userAgent);

                bufferIp.getIp(0, "0");
                buffer.set(ip);
                context.write(buffer, bufferIp);
            } else {
                throw new InvalidInputException(Collections.singletonList(new IOException("Invalid input format! " + line)));
            }
        }
    }

    private void incCounter(Context context, String counterName) {
        context.getCounter("Browsers", counterName).increment(1);
    }
}
