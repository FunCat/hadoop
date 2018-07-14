import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.logging.Logger;

/**
 * The HadoopIpBytes application find the average bytes per request by IP and total bytes by IP.
 * The input format of the data is:
 * example: ip13 - - [24/Apr/2011:04:41:53 -0400] "GET /logs/access_log.3 HTTP/1.1" 200 4846545 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
 * where: ip3 - the number of IP, 200 - HTTP response status, 4846545 - bytes, Mozilla/5.0 - browser.
 *
 * The output format represents as Sequence file compressed with Snappy. The text inside the Sequence file
 * combine in CSV format:
 * IP,average_bytes,total_bytes
 * Example:
 * ip1,25819.02,2762635
 *
 * For running application you need to compile it by the following command:
 * > gradle :ip-request-bytes:clean :ip-request-bytes:build
 *
 * After that you need to run the application on the hadoop environment. Before the start put
 * the input file to the hdfs and check that the directory for the output doesn't exist.
 * Run the following command:
 * > hadoop jar [path_to_the_jar] [path_to_the_input_file] [path_to_the_output_directory]
 * Example:
 * > hadoop jar ip-request-bytes-1.0-SNAPSHOT.jar /input /output
 *
 * To read the output Sequence file use the following command:
 * > hadoop fs -text [path_to_the_output_file]
 * Example:
 * > hadoop fs -text hadoop/IpRequestBytes/output/part-r-00000.snappy
 */
public class HadoopIpBytes extends Configured implements Tool {

    private static Logger log = Logger.getLogger(HadoopIpBytes.class.getName());

    public static void main(String[] args) throws Exception {
        log.info("Starting...");
        int res = ToolRunner.run(new HadoopIpBytes(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        getConf().set(TextOutputFormat.SEPERATOR, ",");
        getConf().setBoolean("mapred.output.compress", true);
        getConf().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(getConf());
        job.setJarByClass(HadoopIpBytes.class);
        job.setJobName("HadoopIpBytes");

        job.setSortComparatorClass(IpWritableComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IpWritable.class);

        job.setMapperClass(IpBytesMapper.class);
        job.setCombinerClass(IpBytesCombiner.class);
        job.setReducerClass(IpBytesReducer.class);

        int returnValue = job.waitForCompletion(true) ? 0 : 1;
        log.info("Job finished Successful - " + job.isSuccessful());

        return returnValue;
    }
}
