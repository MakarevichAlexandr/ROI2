package mapred.graphanalysis;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountFollowers extends Configured implements Tool {

    /*
     * MAPPER
     */
    public static class CountMapper extends Mapper<Text, Text, Text, IntWritable> {

        static enum Counters {
            INPUT_WORDS
        }

        private long numRecords = 0;
        private Map<String, Integer> results = new HashMap<String, Integer>();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //context.getCounter(Counters.INPUT_WORDS).increment(1);
	    String user_id = key.toString();
	    Integer count = results.get(user_id);
	    if (count == null) {
		results.put(user_id, 1);
	    } else {
		results.put(user_id, ++count);
	    }

            if ((++numRecords % 1000) == 0) {
                context.setStatus("Finished processing " + numRecords + " records");
                emitResults(context);
            }
        }

        private void emitResults(Context context) throws IOException, InterruptedException {
            for (Entry<String, Integer> counts : results.entrySet()) {
                context.write(new Text(counts.getKey()), new IntWritable(counts.getValue()));
            }
            results.clear();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            emitResults(context);
        }
    }

    /*
     * REDUCER
     */
    public static class CountReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(result, key);
        }
    }

    /*
     * APPLICATION
     */
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        if (args.length != 3) {
            System.err.println("Usage: CountFollowers <input_path> <output_path_for_CountFollowers> <output_path_for_DistributionCountF>");
            System.exit(2);
        }

        Job job = new Job(conf);
        job.setJarByClass(CountFollowers.class);
        job.setJobName("count followers");

        job.setMapperClass(CountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //job.setCombinerClass(CountReducer.class);
        //job.setSortComparatorClass(BigramComparator.class);

        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.waitForCompletion(true);

	Configuration conf2 = getConf();
	Job job2 = new Job(conf2);
	job2.setJarByClass(CountFollowers.class);
	job2.setJobName("distribution count followers");

	job2.setMapperClass(CountMapper.class);
	job2.setMapOutputKeyClass(Text.class);
	job2.setMapOutputValueClass(IntWritable.class);

	//job2.setCombinerClass(CountReducer.class);

	job2.setReducerClass(CountReducer.class);
	job2.setOutputKeyClass(IntWritable.class);
	job2.setOutputValueClass(Text.class);

	job2.setInputFormatClass(KeyValueTextInputFormat.class);
	job2.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.addInputPath(job2, new Path(args[1]));
	FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        boolean success = job2.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new CountFollowers(), args);
        System.exit(ret);
    }
}
