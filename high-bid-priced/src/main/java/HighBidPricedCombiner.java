import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * HighBidPricedCombiner get the information about each city and how much is the bidding price.
 * So, HighBidPricedCombiner groups the cities by their names and calculate the amount of
 * bidding price higher than 250 for each city.
 *
 * For example:
 * INPUT:
 * [
 *      {cityId: "218", counter: 294, os: "Windows NT 4"},
 *      {cityId: "218", counter: 268, os: "Windows NT 4"},
 *      {cityId: "225", counter: 277, os: "Windows NT 4"}
 * ]
 * OUTPUT
 * [
 *      {cityId: "218", counter: 2, os: "Windows NT 4"},
 *      {cityId: "225", counter: 1, os: "Windows NT 4"}
 * ]
 */
public class HighBidPricedCombiner extends Reducer<Text, CityWritable, Text, CityWritable> {

    private static Logger log = Logger.getLogger(HighBidPricedCombiner.class.getName());
    private CityWritable bufferCity = new CityWritable();

    @Override
    protected void reduce(Text key, Iterable<CityWritable> values, Context context) throws IOException, InterruptedException {
        int counter = 0;

        CityWritable first = values.iterator().next();
        int cityId = first.getCityId().get();
        int highBidPrice = first.getCounter().get();
        String os = first.getOs().toString();

        if (highBidPrice > 250) {
            counter++;
        }

        for (CityWritable value : values) {
            highBidPrice = value.getCounter().get();
            if (highBidPrice > 250) {
                counter++;
            }
        }

        if (counter > 0) {
            log.info("COMBINER: City: " + key + ", counter: " + counter);
            bufferCity.getCity(cityId, counter, os);
            context.write(key, bufferCity);
        }
    }
}
