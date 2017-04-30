import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InitialMaxLen {

	public static void main(String[] args) throws Exception {

        // create new job
		Job job = Job.getInstance(new Configuration());

        // job is based on jar containing this class
        job.setJarByClass(InitialMaxLen.class);

        // for logging purposes
        job.setJobName("WordCount");

        // set input path in HDFS
	    FileInputFormat.addInputPath(job, new Path(args[0]));

        // set output path in HDFS (destination must not exist)
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set mapper and reducer classes
	    job.setMapperClass(MyMapper.class);
	    job.setReducerClass(MyReducer.class);

        // An InputFormat for plain text files.
        // Files are broken into lines. Either linefeed or carriage-return are used
        // to signal end of line. Keys are the position in the file, and values
        // are the line of text.
	    job.setInputFormatClass(TextInputFormat.class);

        // set type of output keys and values for both mappers and reducers
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

        // start job
	    job.waitForCompletion(true);
	}

    // mapper class
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			Scanner s = new Scanner(value.toString());
			s.useDelimiter("\\W"); // any non-alphanumeric character is a delimiter

            // emit <key,value> pairs of the form <word, 1> for each word on the libe
			while (s.hasNext()) {
                String word = s.next().toLowerCase();
                String first = "";
                first = first + word.charAt(0);
				context.write(new Text(first), new IntWritable(word.length()));
            }
       	}
	}

    // reducer class
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int max = 0;
			for (IntWritable value: values) {
                int len = value.get();
                if(len > max)
                    max = len;
            }
			context.write(key, new IntWritable(max));
		}
	}
}
