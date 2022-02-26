package mapred.bigram;

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

public class BigramTop extends Configured implements Tool {

    /*
     * MAPPER
     */
    public static class TopMapper extends Mapper<Text, Text, Text, IntWritable> {

	static Integer bg_repeat;
	private Map<Text, IntWritable> results = new HashMap<Text, IntWritable>();

        @Override
        protected void setup(Context context) {
	    bg_repeat = Integer.parseInt(context.getConfiguration().get("bigramtop.bg-repeat"));
        }

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

	    Integer val = Integer.parseInt(value.toString());
	    if(val >= bg_repeat) {
		context.write(key, new IntWritable(val));
	    }
      	}
    }

    /*
     * REDUCER
     */
    public static class TopReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	static Integer lines_count;
	private IntWritable result = new IntWritable();

	@Override
	protected void setup(Context context) {
	    lines_count = Integer.parseInt(context.getConfiguration().get("bigramtop.lines-count"));
	}

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {

	    if (lines_count-- > 0) {
		int sum = 0;
		for (IntWritable val : values) {
		    sum += val.get();
		}

		result.set(sum);
		context.write(key, result);
	    }
      	}
    }

    /*
     * APPLICATION
     */
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
	if(args.length != 4) {
	    System.err.println("Usage: BigramTop <input_path> <output_path> <bg_repeat> <lines_count>");
	    System.exit(2);
	}

	conf.set("bigramtop.bg-repeat", args[2]);
	conf.set("bigramtop.lines-count", args[3]);

        Job job = new Job(conf);
        job.setJarByClass(BigramTop.class);
        job.setJobName("bigram topn");

        job.setMapperClass(TopMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

	job.setCombinerClass(TopReducer.class);

        job.setReducerClass(TopReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new BigramTop(), args);
        System.exit(ret);
    }
}
