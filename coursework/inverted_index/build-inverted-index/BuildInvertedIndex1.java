/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pdccourse.hw3;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import java.util.StringTokenizer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BuildInvertedIndex {

  public static class MPair implements WritableComparable<MPair> {
    private String word;
    private Long count;

    public void set(String word, Long count) {
      this.word = word;
      this.count = count;
    }

    public String getWord() {
      return word;
    }

    public Long getCount() {
      return count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      word = Text.readString(in);
      count = Long.parseLong(Text.readString(in));
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, word);
      Text.writeString(out, count.toString());
    }

    @Override
    public int hashCode() {
      return word.hashCode() + count.hashCode();
    }

    @Override
    public String toString() {
      return word.toString() + " " + count.toString();
    }

    @Override
    public int compareTo(MPair pair) {
      int compareValue = this.word.compareTo(pair.getWord());
      if (compareValue == 0) {
        compareValue = this.count.compareTo(pair.getCount());
      }
      return (-1) * compareValue;
    }
  }

  public static class MPairPartitioner extends Partitioner<MPair, Text> {
    @Override
    public int getPartition(MPair pair, Text text, int numberOfPartitions) {
      return Math.abs(pair.getWord().hashCode() % numberOfPartitions);
    }
  }

  public static class MPairGroupingComparator extends WritableComparator {
    public MPairGroupingComparator() {
      super(MPair.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
      MPair pair = (MPair) wc1;
      MPair pair2 = (MPair) wc2;
      return pair.getWord().compareTo(pair2.getWord());
    }
  }

  public static class TokenizerMapper
       extends Mapper<Text, Text, MPair, Text> {

    private long numRecords = 0;
    private Map<String, Map<String, Long> > results = new HashMap<String, Map<String, Long> >();
    private final MPair pair = new MPair();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
	String word = itr.nextToken();
	Map<String, Long> counts = results.get(word);
	if (counts == null) {
	  counts = new HashMap<String, Long>();
	  results.put(word, counts);
	}

	String doc_id = key.toString();
	Long count = counts.get(doc_id);
	if (count == null) {
	  counts.put(doc_id, Long.valueOf(1));
	} else {
	  counts.put(doc_id, ++count);
	}
        //word.set(itr.nextToken());
        //context.write(word, one);
      }

      if ((++numRecords % 1000) == 0) {
	context.setStatus("Finished processing " + numRecords + " records");
	emitResults(context);
      }
    }

    private void emitResults(Context context) throws IOException, InterruptedException {
      for (Entry<String, Map<String, Long> > counts : results.entrySet()) {
        String word = counts.getKey();
	for (Entry<String, Long> count : counts.getValue().entrySet()) {
	  pair.set(word, count.getValue());
	  context.write(pair, new Text(count.getValue().toString()));
	}
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      emitResults(context);
    }
  }

  public static class IntSumReducer
       extends Reducer<MPair, Text, MPair, Text> {
    //private IntWritable result = new IntWritable();

    public void reduce(MPair key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String doc_id = null;
      for (Text val : values) {
        doc_id += val.toString() + "@";
      }
      //result.set(sum);
      context.write(key, new Text(doc_id));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: buildinvertedindex <in> <out>");
      System.exit(2);
    }
    conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
    conf.setBoolean("exact.match.only", true);
    conf.set("io.serializations",
             "org.apache.hadoop.io.serializer.JavaSerialization,"
             + "org.apache.hadoop.io.serializer.WritableSerialization");

    Job job = new Job(conf, "build inverted index");
    job.setInputFormatClass(XmlInputFormat.class);
    job.setJarByClass(BuildInvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);


    job.setPartitionerClass(MPairPartitioner.class);
    job.setGroupingComparatorClass(MPairGroupingComparator.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(MPair.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
