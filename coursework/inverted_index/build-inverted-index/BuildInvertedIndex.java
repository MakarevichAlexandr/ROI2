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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;

import java.util.StringTokenizer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class BuildInvertedIndex {

  public static class MPair implements WritableComparable<MPair> {
    private String word;
    private Integer tf;

    public void set(String word, Integer tf) {
      this.word = word;
      this.tf = tf;
    }

    public String getWord() {
      return word;
    }

    public Integer getTf() {
      return tf;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      word = Text.readString(in);
      tf = Integer.parseInt(Text.readString(in));
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, word);
      Text.writeString(out, tf.toString());
    }

    @Override
    public int hashCode() {
      return word.hashCode();
    }

    @Override
    public String toString() {
      return word.toString();
    }

    @Override
    public int compareTo(MPair o) {
      //String[] wr = word.split(" ");
      //String[] owr = o.word.split(" ");
      if (!word.equals(o.word)) {
          return word.compareTo(o.word);
      } else {
	//if (!wr[1].equals(owr[1])) {
	  return (o.tf > tf ? 1 : -1);
	//}
	//return 0;
      }
    }
  }

  public static class MPartitioner extends Partitioner<MPair, Text> {
    @Override
    public int getPartition(MPair pair, Text docid, int numOfPartitions) {
      return ( pair.getWord().hashCode() & Integer.MAX_VALUE ) % numOfPartitions;
    }
  }

  public static class MGroupComparator extends WritableComparator {
    //private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    //private static final IntWritable.Comparator INTWRITABLE_COMPARATOR = new IntWritable.Comparator();

    public MGroupComparator() {
      super(MPair.class, true);
    }

    /*@Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      try {
	int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
	int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
	int cmp1 = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
	if (cmp1 != 0) {
	  return cmp1;
	} else {
	  int secondL1 = WritableUtils.decodeVIntSize(b1[s1+firstL1]) + readVInt(b1, s1+firstL1);
	  int secondL2 = WritableUtils.decodeVIntSize(b2[s2+firstL2]) + readVInt(b2, s2+firstL2);
	  return (-1) * INTWRITABLE_COMPARATOR.compare(b1, s1+firstL1, secondL1, b2, s2+firstL2, secondL2);
	}
      } catch (IOException e) {
	throw new IllegalArgumentException(e);
      }
    }*/

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      if (w1 instanceof MPair && w2 instanceof MPair) {
	return ((MPair)w1).compareTo((MPair)w2);
      }
      return super.compare(w1, w2);
    }
  }

  public static class TokenizerMapper
       extends Mapper<Text, Text, MPair, Text> {

    private List<String> skipList = new ArrayList<String>();
    private long numRecords = 0;
    private Map<String, Map<String, Integer> > results = new HashMap<String, Map<String, Integer> >();
    private final MPair pair = new MPair();
    private final Text docid = new Text();

    @Override
    protected void setup(Context context) {
      String skipListFile = context.getConfiguration().get("buildinvertedindex.skip-list");
      if (skipListFile != null) {
	loadSkipListFile(skipListFile);
      }
    }

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      String doc_id = key.toString();
      String text = value.toString();

      for (String pattern : skipList) {
	text = text.replaceAll(pattern, " ");
      }

      StringTokenizer itr = new StringTokenizer(text);
      String word;
      while (itr.hasMoreTokens()) {
	word = itr.nextToken();
	addResult(word, doc_id);
      }

      if ((++numRecords % 1000) == 0) {
	context.setStatus("Finished processing " + numRecords + " records");
	emitResults(context);
      }
    }

    private void loadSkipListFile(String skipListFile) {
      BufferedReader fis = null;
      try {
	fis = new BufferedReader(new FileReader(skipListFile));
	String pattern = null;
	while ((pattern = fis.readLine()) != null) {
	  skipList.add(pattern);
	}
      } catch (IOException ioe) {
	System.err.println("Caught exception while loading skip file '" + skipListFile + "' : "
	  + StringUtils.stringifyException(ioe));
      } finally {
	if (fis != null) {
	  try {
	    fis.close();
	  } catch (IOException ioe) {
	    System.err.println("Caught exception while closing skip file '" + skipListFile + "' : "
	      + StringUtils.stringifyException(ioe));
	  }
	}
      }
    }

    private void addResult(String word, String docid) {
      Map<String, Integer> counts = results.get(word);
      if (counts == null) {
	counts = new HashMap<String, Integer>();
	results.put(word, counts);
      }
      Integer count = counts.get(docid);
      if (count == null) {
	counts.put(docid, 1);
      } else {
	counts.put(docid, ++count);
      }
    }

    private void emitResults(Context context) throws IOException, InterruptedException {
      for (Entry<String, Map<String, Integer> > counts : results.entrySet()) {
        String word = counts.getKey();
	for (Entry<String, Integer> count : counts.getValue().entrySet()) {
	  pair.set(word, count.getValue());
	  docid.set(count.getKey());
	  context.write(pair, docid);
	}
      }
      results.clear();
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      emitResults(context);
    }
  }

  public static class IntSumReducer
       extends Reducer<MPair, Text, Text, Text> {
    //private IntWritable result = new IntWritable();
    private Map<String, Map<String, Integer> > results = new HashMap<String, Map<String, Integer> >();
    private Map<String, Long> DD = new HashMap<String, Long>();
    //private final MPair pair = new MPair();
    private final Text word = new Text();
    private final Text docid = new Text();
    private long numRecords = 0;
    static double D;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      D = Double.parseDouble(context.getConfiguration().get("buildinvertedindex.D"));
    }

    public void reduce(MPair key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String docid = "";
      for (Text val : values) {
        docid = val.toString();
      }

      addResult(key.getWord(), docid, key.getTf());
      //context.write(new Text(key.getWord() + " " + key.getTf()), new Text(docid));
      if ((numRecords % 1000) == 0) {
	context.setStatus("Reduce: Finished processing " + numRecords + " records");
	emitResults(context);
      }
    }

    private void addResult(String word, String docid, Integer tf) {
      Map<String, Integer> counts = results.get(word);
      if (counts == null) {
	counts = new LinkedHashMap<String, Integer>();
	results.put(word, counts);
	++numRecords;
      }

      Integer count = counts.get(docid);
      if (count == null) {
	if (counts.size() < 20) {
	  counts.put(docid, tf);
	}
      } else {
	counts.put(docid, count + tf);
      }

      Long dd = DD.get(word);
      if (dd == null) {
	DD.put(word, (long)tf);
      } else {
	DD.put(word, dd + tf);
      }
    }

    private void emitResults(Context context) throws IOException, InterruptedException {
      for (Entry<String, Map<String, Integer> > counts : results.entrySet()) {
	String wr = counts.getKey();
	word.set(wr);

	Long dd = DD.get(wr);
	Double idf = Math.abs(Math.log(D/dd));
	String text = /*dd.toString() + " : " + idf.toString() +*/ " [";
	for (Entry<String, Integer> count : counts.getValue().entrySet()) {
	  text += "<" + count.getKey() + ", " + Double.valueOf(count.getValue() * idf).toString() + ">, ";
	}
	text = text.substring(0, text.length() - 2);
	text += "]";
	docid.set(text);
	context.write(word, docid);
      }
      results.clear();
      DD.clear();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      emitResults(context);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 4) {
      System.err.println("Usage: buildinvertedindex <in> <out> <skip_list> <d>");
      System.exit(2);
    }

    File skipFile = new File(otherArgs[2]);
    conf.set("buildinvertedindex.skip-list", skipFile.getName());
    conf.set("tmpfiles", "file://" + skipFile.getAbsolutePath());
    conf.set("buildinvertedindex.D", otherArgs[3]);

    conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
    conf.setBoolean("exact.match.only", true);
    conf.set("io.serializations",
             "org.apache.hadoop.io.serializer.JavaSerialization,"
             + "org.apache.hadoop.io.serializer.WritableSerialization");

    Job job = new Job(conf, "build inverted index");
    job.setInputFormatClass(XmlInputFormat.class);
    job.setJarByClass(BuildInvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(MPair.class);
    job.setMapOutputValueClass(Text.class);

    //job.setCombinerClass(IntSumReducer.class);

    job.setPartitionerClass(MPartitioner.class);

    job.setSortComparatorClass(MGroupComparator.class);
    //job.setGroupingComparatorClass(MGroupComparator.class);

    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
