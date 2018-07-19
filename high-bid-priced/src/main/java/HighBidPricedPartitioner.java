import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * The HighBidPricedPartitioner determines by which reducer will process the record. As a parameter
 * for splitting between reducers, the HighBidPricedPartitioner uses the name of operation system.
 */
public class HighBidPricedPartitioner extends Partitioner<Text, CityWritable> {

    @Override
    public int getPartition(Text key, CityWritable value, int numReduceTasks) {

        if(numReduceTasks == 0)
            return 0;

        String os = value.getOs().toString();

        if(os.equals("Windows NT 4")){
            return 0;
        }
        if(os.equals("Windows NT 5")){
            return 1 % numReduceTasks;
        }
        if(os.equals("Windows NT 6")){
            return 2 % numReduceTasks;
        }
        else {
            return 3 % numReduceTasks;
        }
    }
}
