package pl.put.idss.hadoop.simpletasks;

import java.io.IOException;
import java.util.HashSet;
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
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class NeighbourCount {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: neighbourcount <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "neighbourcount");
		job.setJarByClass(NeighbourCount.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public class NeigbourMapper extends Mapper<Object, Text, Text, HashSet<Text>>{

		private Text word = new Text();

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] tokens = value.toString().replaceAll("[^a-zA-Z śćóżźąęłĄĘŚĆŹŻŁ]", "").split(" ");
			for(String token : tokens) {
				word.set(token);
				//context.write(word,ONE);
			}

		}
	}

	public class NeighbourReducer extends Reducer<Text, HashSet<Text>, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<HashSet<Text>> values, Context context) throws IOException, InterruptedException {
			super.reduce(key, values, context);
		}
	}

}
