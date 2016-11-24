package pl.put.idss.hadoop.simpletasks;

import java.io.IOException;
import java.util.HashSet;

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

        job.setMapperClass(NeighbourMapper.class);
        //job.setCombinerClass(NeighbourReducer.class);
        job.setReducerClass(NeighbourReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public class NeighbourMapper extends Mapper<Object, Text, Text, HashSet<Text>>{

        private Text createNewText(String text){
            return new Text(text);
        }

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] tokens = value.toString().replaceAll("[^a-zA-Z śćóżźąęłĄĘŚĆŹŻŁ]", "").split(" ");

            for (int i = 0; i<tokens.length; i++)
            {
                HashSet<Text> neighbours = new HashSet<>();

                if(i-1 > -1)
                    neighbours.add(createNewText(tokens[i-1]));

                if(i+1 < tokens.length)
                    neighbours.add(createNewText(tokens[i+1]));

                context.write(createNewText(tokens[i]), neighbours);
            }
		}
	}

	public class NeighbourReducer extends Reducer<Text, HashSet<Text>, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<HashSet<Text>> values, Context context) throws IOException, InterruptedException {

            HashSet<Text> mergedSet = new HashSet<>();

            for (HashSet<Text> neighboursSet : values)
                mergedSet.addAll(neighboursSet);

            context.write(key, new IntWritable(mergedSet.size()));
		}
	}

}
