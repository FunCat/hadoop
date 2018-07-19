import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.logging.Logger;

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

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CityWritable.class);

        job.setMapperClass(HighBidPricedMapper.class);
        job.setCombinerClass(HighBidPricedReducer.class);
        job.setReducerClass(HighBidPricedReducer.class);
        job.setPartitionerClass(HighBidPricedPartitioner.class);

        job.setNumReduceTasks(4);

        DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());

        int returnValue = job.waitForCompletion(true) ? 0 : 1;
        log.info("Job finished Successful - " + job.isSuccessful());

        return returnValue;
    }
}
