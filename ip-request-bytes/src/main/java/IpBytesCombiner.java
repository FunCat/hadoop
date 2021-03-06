import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * IpBytesCombiner get the information about each ip and how many bytes they received.
 * So, IpBytesCombiner groups the ip by their names and calculate the average number of bytes and
 * the total number for the each ip group.
 *
 * For example:
 * INPUT:
 * [
 *      {ip: "ip1", avgBytes: 0.0, count: 1, bytes: 514},
 *      {ip: "ip2", avgBytes: 0.0, count: 1, bytes: 654},
 *      {ip: "ip2", avgBytes: 0.0, count: 1, bytes: 358}
 * ]
 * OUTPUT
 * [
 *      {ip: "ip1", avgBytes: 0.0, count: 1, bytes: 514},
 *      {ip: "ip2", avgBytes: 0.0, count: 2, bytes: 1012}
 * ]
 */
public class IpBytesCombiner extends Reducer<Text, IpWritable, Text, IpWritable> {

    private static Logger log = Logger.getLogger(IpBytesCombiner.class.getName());
    private IpWritable bufferIp = new IpWritable();

    @Override
    protected void reduce(Text key, Iterable<IpWritable> values, Context context) throws IOException, InterruptedException {
        long total = 0;
        int size = 0;

        for (IpWritable value : values) {
            total += value.getBytes().get();
            size += value.getCount().get();
        }

        bufferIp.getIp(0, size, String.valueOf(total));
        log.info("COMBINER: Ip: " + key + ", avg bytes: 0, count: " + size + ", total bytes: " + total);
        context.write(key, bufferIp);
    }
}
