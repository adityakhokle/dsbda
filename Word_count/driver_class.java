import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class driver_class {

	public static void main(String[] args) throws Exception{

		// TODO Auto-generated method stub

			Configuration c=new Configuration();
			@SuppressWarnings("deprecation")
			Job job=new Job(c,"WordCount");
			job.setJarByClass(driver_class.class);
			job.setMapperClass(mapper_class.class);
			job.setReducerClass(reducer_class.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);

	}


}


