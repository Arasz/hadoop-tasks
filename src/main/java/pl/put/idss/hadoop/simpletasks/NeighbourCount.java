package pl.put.idss.hadoop.simpletasks;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
        job.setCombinerClass(NeighbourCollector.class);
        job.setReducerClass(NeighbourReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static class TextArrayWritable extends ArrayWritable{

        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(Text[] values) {
            super(Text.class, values);
        }

        public TextArrayWritable(Collection<Text> values) {
            super(Text.class, values.toArray(new Text[values.size()]));
        }

        public TextArrayWritable(String[] strings) {
            super(strings);
        }

        @Override
        public Text[] get() {
            return (Text[]) super.get();
        }
    }

    private static  TextArrayWritable convertToWritableArray(HashSet<Text> hashSet)
    {
        return new TextArrayWritable(hashSet);
    }

	public static class NeighbourMapper extends Mapper<Object, Text, Text, TextArrayWritable>{

        private Text createNewText(String text){
            return new Text(text);
        }



		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] tokens = value.toString().replaceAll("[^a-zA-Z śćóżźąęłĄĘŚĆŹŻŁ]", "").split(" ");

            for (int i = 0; i<tokens.length; i++)
            {
                HashSet<Text> neighbours = new HashSet<>();

                if(i-1 > -1)
                    neighbours.add(createNewText(tokens[i-1]));

                if(i+1 < tokens.length)
                    neighbours.add(createNewText(tokens[i+1]));

                context.write(createNewText(tokens[i]), convertToWritableArray(neighbours));
            }
		}
	}

	public static class NeighbourCollector extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable>
	{
		@Override
		public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            HashSet<Text> mergedSet = new HashSet<>();

            for (ArrayWritable neighboursSet : values)
                for (String text : neighboursSet.toStrings())
                    mergedSet.add(new Text(text));

			context.write(key, convertToWritableArray(mergedSet));
		}
	}

	public static class NeighbourReducer extends Reducer<Text, TextArrayWritable, Text, IntWritable>{

		@Override
		public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {

            HashSet<String> mergedSet = new HashSet<>();

            for (TextArrayWritable neighboursSet : values)
                for (String text : neighboursSet.toStrings())
                    mergedSet.add(text);

            context.write(key, new IntWritable(mergedSet.size()));
		}
	}

}
