import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.logging.Logger;

public class HighBidPricedReducer extends Reducer<Text, CityWritable, Text, CityWritable> {

    private static Logger log = Logger.getLogger(HighBidPricedReducer.class.getName());
    private CityWritable bufferCity = new CityWritable();

    @Override
    protected void reduce(Text key, Iterable<CityWritable> values, Context context) throws IOException, InterruptedException {

        CityWritable first = values.iterator().next();
        int cityId = first.getCityId().get();
        int highBidPrice = first.getBiddingPrice().get();
        String os = first.getOs().toString();

        for (CityWritable value : values) {
            highBidPrice = Math.max(value.getBiddingPrice().get(), highBidPrice);
        }

        if (highBidPrice != -1) {
            bufferCity.getCity(cityId, highBidPrice, os);
            log.info("REDUCER: City: " + key + ", high bid price: " + highBidPrice);
            context.write(key, bufferCity);
        }
    }
}
