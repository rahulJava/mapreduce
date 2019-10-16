import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Index {

	public static class SimpleInvertedIndexMapper extends
			Mapper<Object, Text, Text, Text> {

		/*
		 * @Text : stores the combination of term and documents list
		 * 
		 * @fileSplit: is used to get the name of the input files
		 * 
		 * @stringTokenizer: is used to tokenize the whole data in to seperate
		 * word line by line
		 */
		private Text word = new Text();
		Text docIDList = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();

			docIDList = new Text(filename);
			/*
			 * get the FileSplit tatget of <key,value> "value" saves one line of
			 * message in the txt file "key" is the offset of the initial and
			 * the head address of txt file
			 */
			line = line.replaceAll("\\p{Punct}", " ");
			line = line.replaceAll("\\d", " ");
			line = line.toLowerCase();

			/*
			 * A Simple inverted index mapper just needs to have key value pair
			 * of term as a key and doc name as a value. iterating string
			 * tokenizer gives us the word , which acts a key. And docname we
			 * got from filesplit api above will be saved as value here.
			 */
			StringTokenizer itr = new StringTokenizer(line);

			while (itr.hasMoreTokens()) {

				word.set(itr.nextToken());
				context.write(word, docIDList);
			}
		}

	}

	public static class SimpleInvertedIndexReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text keyword, Iterable<Text> docIDList,
				Context output) throws IOException, InterruptedException {

			/*
			 * Creating hashset and adding all the values in docList to make
			 * sure there is no duplicacy in the list.
			 */
			Iterator<Text> itr = docIDList.iterator();
			Set<Text> outputValues = new HashSet<Text>();
			ArrayList<Text> al = new ArrayList<Text>();
			int count = 0;
			String docID = new String();
			String docString = "";

			while (itr.hasNext()) {

				Text value = new Text(itr.next());
				outputValues.add(value);
			}
			boolean first = true;
			StringBuilder toReturn = new StringBuilder();
			/*
			 * Creating an iterator to iterate hashset and appending all the
			 * values in docs list to form a single string containing all the
			 * values.
			 */
			Iterator<Text> outputIterator = outputValues.iterator();
			for (Text t1 : outputValues) {
				al.add(t1);
			}
			/*
			 * Sorting the document list as per the requirement
			 */
			Collections.sort(al);
			for (Text input : al) {
				if (!first) {
					toReturn.append(",");
				}
				first = false;
				toReturn.append(input.toString());
			}

			output.write(keyword, new Text(toReturn.toString()));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Inverted Index");

		job.setJarByClass(Index.class);
		// use BiWordIndex class to finish Mapping
		job.setMapperClass(SimpleInvertedIndexMapper.class);
		// set the output format of Mapping and Reduction, key's is Text
		job.setReducerClass(SimpleInvertedIndexReducer.class);
		// set the input format of Mapping and Reduction, value's is Text；
		job.setOutputKeyClass(Text.class);
		// set the input address of mission data
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		// call job.waitForCompletion(true) to finish the misson
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
