import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.logging.Logger;

/**
 * The HadoopHighBidPriced application count the amount of high-bid-priced which is more than 250
 * for each city. It uses the file with lookup which used for the mapping the city id to the city name.
 * Also the HadoopHighBidPriced uses the custom {@link HighBidPricedPartitioner} which allows us to
 * write our output to the different files and grouping the records by the Operation System.
 *
 * The input format of the data is:
 * example: 2e72d1bd7185fb76d69c852c57436d37	20131019025500549	1	CAD06D3WCtf	Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)	113.117.187.*	216	234	2	33235ca84c5fee9254e6512a41b3ad5e	8bbb5a81cc3d680dd0c27cf4886ddeae	null	3061584349	728	90	OtherView	Na	5	7330	277	48	null	2259	10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063
 * where: 234 - the city id, 277 - bidding price.
 *
 * The output format represents the city name and the amount of high-bid-priced which is more than 250.
 * Example:
 * aba	607
 *
 * For running application you need to compile it by the following command:
 * > gradle :high-bid-priced:clean :high-bid-priced:build
 *
 * After that you need to run the application on the hadoop environment. Before the start put
 * the input file to the hdfs and check that the directory for the output doesn't exist.
 * Run the following command:
 * > hadoop jar [path_to_the_jar] [path_to_the_input_file] [path_to_the_output_directory] [path_to_the_lookup_file]
 * Example:
 * > hadoop jar ip-request-bytes-1.0-SNAPSHOT.jar /input /output /city.en.txt
 */
public class HadoopHighBidPriced extends Configured implements Tool {

    private static Logger log = Logger.getLogger(HadoopHighBidPriced.class.getName());

    public static void main(String[] args) throws Exception {
        log.info("Starting...");
        int res = ToolRunner.run(new HadoopHighBidPriced(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s needs three arguments, input, output files and file with names of the cities\n", getClass().getSimpleName());
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("HadoopHighBidPriced");
        job.setJarByClass(HadoopHighBidPriced.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CityWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(HighBidPricedMapper.class);
        job.setCombinerClass(HighBidPricedCombiner.class);
        job.setReducerClass(HighBidPricedReducer.class);
        job.setPartitionerClass(HighBidPricedPartitioner.class);

        job.setNumReduceTasks(4);

        DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());

        int returnValue = job.waitForCompletion(true) ? 0 : 1;
        log.info("Job finished Successful - " + job.isSuccessful());

        return returnValue;
    }
}
