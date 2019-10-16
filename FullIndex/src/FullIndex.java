import java.io.IOException;
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

public class FullIndex {

	public static class FullInvertedIndexMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		/*
		 * @Text : stores the combination of term and documents list
		 * @fileSplit: is used to get the name of the input files
		 * @stringTokenizer: is used to tokenize the whole data in to seperate word line by line
		 */

		private Text word = new Text();
		Text docID = new Text();

		public void map(LongWritable offset, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			docID = new Text(filename);
			/*get the FileSplit tatget of <key,value>
            "value" saves one line of message in the txt file
            "key" is the offset of the initial and the head address of txt file
			*/
			line = line.replaceAll("\\p{Punct}", " ");
			line = line.replaceAll("\\d", " ");
			line = line.toLowerCase();

			StringTokenizer itr = new StringTokenizer(line);
			/*
			 * To create a full index we need to add offset of the word  
			 * in the dictionary . And to implement that we need to count the postion
			 * of the word in the distributed file . initially we have taken an integer 
			 * wordcount and initialized that with "0". And while iterating the tokenizer 
			 * we keep on increasing the wordcount . And adding the wordcount along with the file name to
			 * get the desired output. 
			 * 
			 */
			int wordcount = 0;
			while (itr.hasMoreTokens()) {

				word.set(itr.nextToken());
				context.write(word,
						new Text(docID + "@" + offset));
				//wordcount = wordcount + 1;
			}
		}

	}

	public static class FullInvertedIndexReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text keyword, Iterable<Text> docIDList,
				Context output) throws IOException, InterruptedException {

			/*
			 * Creating hashset and adding all the values in docList to make
			 * sure there is no duplicacy in the list.
			 */
			Iterator<Text> itr = docIDList.iterator();
			Set<Text> outputValues = new HashSet<Text>();
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
			 * Creating an iterator to iterate hashset and 
			 * appending all the values in docs list to form a single
			 * string containing all the values.
			 */
			Iterator<Text> outputIter = outputValues.iterator();
			while (outputIter.hasNext()) {
				if (!first) {
					toReturn.append("+");
				}
				first = false;
				toReturn.append(outputIter.next().toString());
			}
			output.write(keyword, new Text(toReturn.toString()));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "FullInverted Index");

		job.setJarByClass(FullIndex.class);
		//use FullIndex class to finish Mapping
		job.setMapperClass(FullInvertedIndexMapper.class);
		//set the output format of Mapping and Reduction, key's is Text
		job.setReducerClass(FullInvertedIndexReducer.class);
		//set the input format of Mapping and Reduction, value's is Text
		job.setOutputKeyClass(Text.class);
		//set the input address of mission data
		job.setOutputValueClass(Text.class);
		conf.set("mapreduce.output.textoutputformat.separator", " ");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//call job.waitForCompletion(true) to finish the misson
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
