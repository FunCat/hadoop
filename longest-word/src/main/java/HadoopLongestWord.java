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

/**
 * The HadoopLongestWord application find the longest word in th input file and return the longest
 * word with its length. It can be a several words, if they have the same longest length.
 *
 * For running application you need to compile it by the following command:
 * > gradle :longest-word:clean :longest-word:build
 *
 * After that you need to run the application on the hadoop environment. Before the start put
 * the input file to the hdfs and check that the directory for the output doesn't exist.
 * Run the following command:
 * > hadoop jar [path_to_the_jar] [path_to_the_input_file] [path_to_the_output_directory]
 * Example:
 * > hadoop jar longest-word-1.0-SNAPSHOT.jar /input.txt /output
 *
 */
public class HadoopLongestWord extends Configured implements Tool {

    private static Logger log = Logger.getLogger(HadoopLongestWord.class.getName());

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
        job.setCombinerClass(LongestWordCombiner.class);
        job.setReducerClass(LongestWordReducer.class);

        int returnValue = job.waitForCompletion(true) ? 0 : 1;
        log.info("Job finished Successful - " + job.isSuccessful());

        return returnValue;
    }
}
