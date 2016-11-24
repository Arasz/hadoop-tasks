package pl.put.idss.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	/**
	 * Mapper implementation
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		// in WordCount we emit "1" for a given token...
		private static final IntWritable ONE = new IntWritable(1);

		// use this variable to store the token that you want to emit (word.set(...))
		private Text word = new Text();

		// key and value in the map procedure
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Use string tokenizer (java.util.StringTokenizer)
			// Emit data to reducer by context.write(key,value)

			String[] tokens = value.toString().replaceAll("[^a-zA-Z śćóżźąęłĄĘŚĆŹŻŁ]", "").split(" ");
            for(String token : tokens) {
                word.set(token);
                context.write(word,ONE);
            }

		}

	}

	/**
	 * Reducer implementation
	 */
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// use this variable to store the sum of tokens (result.set(...))
		private IntWritable result = new IntWritable();

		// key and list of values for this key in the reduce procedure
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			// for each element in 'values'...
			// ... do something ...
			// and emit (context.write(...))
            int sum = 0;
            for(IntWritable count : values)
                sum+=count.get();

            result.set(sum);
            context.write(key,result);
		}
	}

	public static void main(String[] args) throws Exception {
		//Configuration for our Job
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//We can show a help message...
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		//Creating of our Job
		Job job = new Job(conf, "wordcount");
		//...and we have to set class of our program for the Job
		job.setJarByClass(WordCount.class);

		//...class of mapper
		job.setMapperClass(TokenizerMapper.class);
		//...combiner - what is it?
		// The Combiner is a "mini-reduce" process which operates only on data generated by one machine.
		job.setCombinerClass(IntSumReducer.class);
		//...and class for reducer
		job.setReducerClass(IntSumReducer.class);

		//Set types of our output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//...and we take the paths to the input/output files from the parameters' array
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		//Finally, exit the program for our Job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}