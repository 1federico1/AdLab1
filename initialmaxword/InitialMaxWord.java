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


public class InitialMaxWord {

	public static void main(String[] args) throws Exception {

        // create new job
		Job job = Job.getInstance(new Configuration());

        // job is based on jar containing this class
        job.setJarByClass(InitialMaxWord.class);

        // for logging purposes
        job.setJobName("InitialMaxWord");

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
	    job.setOutputValueClass(Text.class);

        // start job
	    job.waitForCompletion(true);
	}

    // mapper class
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

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
				context.write(new Text(first), new Text(word));
            }
       	}
	}

    // reducer class
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Text max = null;
            int maxlen = 0;
			for (Text value: values) {
                int len = value.toString().length();
                if(len > maxlen) {
                    maxlen = len;
                    max = new Text(value);
                    //system.out.println
                }
            }
			context.write(key, max);

		}
	}
}
