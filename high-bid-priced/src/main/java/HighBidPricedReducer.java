import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * HighBidPricedReducer get the information about each city group and calculate the total count of
 * high-bid-priced which is more than 250.
 *
 * For example:
 * INPUT:
 * [
 *      {cityId: "218", counter: 12, os: "Windows NT 4"},
 *      {cityId: "218", counter: 54, os: "Windows NT 4"},
 *      {cityId: "225", counter: 9, os: "Windows NT 4"}
 * ]
 * OUTPUT
 * [
 *      {cityId: "218", counter: 66, os: "Windows NT 4"},
 *      {cityId: "225", counter: 9, os: "Windows NT 4"}
 * ]
 */
public class HighBidPricedReducer extends Reducer<Text, CityWritable, Text, IntWritable> {

    private static Logger log = Logger.getLogger(HighBidPricedReducer.class.getName());
    private IntWritable bufferInt = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<CityWritable> values, Context context) throws IOException, InterruptedException {
        int counter = 0;

        for (CityWritable value : values) {
            counter += value.getCounter().get();
        }

        if (counter > 0) {
            log.info("REDUCER: City: " + key + ", counter: " + counter);
            bufferInt.set(counter);
            context.write(key, bufferInt);
        }
    }
}
