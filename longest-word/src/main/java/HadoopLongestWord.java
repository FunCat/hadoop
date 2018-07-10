import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.logging.Logger;

public class HadoopLongestWord extends Configured implements Tool {

    static Logger log = Logger.getLogger(HadoopLongestWord.class.getName());

    public static void main(String[] args) throws Exception {
        log.info("Starting...");
        int res = ToolRunner.run(new HadoopLongestWord(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        Job job = new Job();
        job.setJarByClass(HadoopLongestWord.class);
        job.setJobName("HadoopLongestWord");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(LongestWordMapper.class);
        job.setReducerClass(LongestWordReducer.class);

        int returnValue = job.waitForCompletion(true) ? 0 : 1;
        log.info("Job finished Successful - " + job.isSuccessful());

        return returnValue;
    }
}
