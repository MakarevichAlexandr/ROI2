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
import org.apache.hadoop.io.DoubleWritable;
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

public class AvgCountFollowers extends Configured implements Tool {

    /*
     * MAPPER
     */
    public static class CountMapper extends Mapper<Text, Text, IntWritable, IntWritable> {

        private long numRecords = 0;
        private Map<Integer, Integer> results = new HashMap<Integer, Integer>();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //context.getCounter(Counters.INPUT_WORDS).increment(1);
	    Integer followers_count = Integer.parseInt(value.toString());
	    Integer count = results.get(followers_count);
	    if (count == null) {
		results.put(followers_count, 1);
	    } else {
		results.put(followers_count, ++count);
	    }

            if ((++numRecords % 1000) == 0) {
                context.setStatus("Finished processing " + numRecords + " records");
                emitResults(context);
            }
        }

        private void emitResults(Context context) throws IOException, InterruptedException {
            for (Entry<Integer, Integer> counts : results.entrySet()) {
                context.write(new IntWritable(counts.getKey()), new IntWritable(counts.getValue()));
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
    public static class CountReducer extends Reducer<IntWritable, IntWritable, Text, DoubleWritable> {

	static long val_followers;
	static long count;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
	    val_followers = 0;
	    count = 0;
	}

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

	    val_followers += sum;
	    ++count;
            //context.write(new Text(new String(Integer.toString(key.get()) + "/" + Integer.toString(sum))),
		//new DoubleWritable((double)key.get() / sum));
        }

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
	    context.write(null, new DoubleWritable((double)val_followers / count));
	}
    }

    /*
     * APPLICATION
     */
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        if (args.length != 2) {
            System.err.println("Usage: AvgCountFollowers <input_path> <output_path>");
            System.exit(2);
        }

        Job job = new Job(conf);
        job.setJarByClass(AvgCountFollowers.class);
        job.setJobName("avg count followers");

        job.setMapperClass(CountMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //job.setCombinerClass(CountReducer.class);
        //job.setSortComparatorClass(BigramComparator.class);

        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new AvgCountFollowers(), args);
        System.exit(ret);
    }
}
