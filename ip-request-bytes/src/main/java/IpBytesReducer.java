import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * IpBytesReducer get the information about each ip group and calculate the average number of bytes
 * and the total number for the same ip groups, if they were received for the different combiners.
 *
 * For example:
 * INPUT:
 * [
 *      {ip: "ip1", avgBytes: 0.0, count: 1, bytes: 514},
 *      {ip: "ip2", avgBytes: 0.0, count: 2, bytes: 1012},
 *      {ip: "ip2", avgBytes: 0.0, count: 2 bytes: 214}
 * ]
 * OUTPUT
 * [
 *      {ip: "ip1", avgBytes: 514.0, count: 1, bytes: 514},
 *      {ip: "ip2", avgBytes: 306.5, count: 4, bytes: 1226}
 * ]
 */
public class IpBytesReducer extends Reducer<Text, IpWritable, Text, IpWritable> {

    private static Logger log = Logger.getLogger(IpBytesReducer.class.getName());
    private IpWritable bufferIp = new IpWritable();

    @Override
    protected void reduce(Text key, Iterable<IpWritable> values, Context context) throws IOException, InterruptedException {
        long total = 0;
        int size = 0;

        for (IpWritable value : values) {
            total += value.getBytes().get();
            size += value.getCount().get();
        }

        if (size != 0) {
            float avgBytes = (float) total / size;
            bufferIp.getIp(avgBytes, size, String.valueOf(total));
            log.info("REDUCER: Ip: " + key + ", avg bytes: " + avgBytes + ", total bytes: " + total);
            context.write(key, bufferIp);
        }
    }
}
