import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * IpBytesReducer get the information about each ip group and calculate the average number of bytes
 * and the total number for the same ip groups, if they were received fro the different combiners.
 *
 * For example:
 * INPUT:
 * [
 *      {ip: "ip1", avgBytes: 514.0, bytes: 514},
 *      {ip: "ip2", avgBytes: 506.0, bytes: 1012},
 *      {ip: "ip2", avgBytes: 104.5, bytes: 214}
 * ]
 * OUTPUT
 * [
 *      {ip: "ip1", avgBytes: 514.0, bytes: 514},
 *      {ip: "ip2", avgBytes: 305.25, bytes: 1226}
 * ]
 */
public class IpBytesReducer extends Reducer<Text, IpWritable, Text, IpWritable> {

    private static Logger log = Logger.getLogger(IpBytesReducer.class.getName());
    private IpWritable bufferIp = new IpWritable();

    @Override
    protected void reduce(Text key, Iterable<IpWritable> values, Context context) throws IOException, InterruptedException {
        long total = 0;
        float avg = 0;
        int size = 0;

        for (IpWritable value : values) {
            total += value.getBytes().get();
            avg += value.getAvgBytes().get();
            size++;
        }

        if (size != 0) {
            float avgBytes = avg / size;
            bufferIp.getIp(key.toString(), avgBytes, String.valueOf(total));
            log.info("REDUCER: Ip: " + key + ", avg bytes: " + avgBytes + ", total bytes: " + total);
            context.write(key, bufferIp);
        }
    }
}
