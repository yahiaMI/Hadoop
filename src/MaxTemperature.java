/*MaxTemperature Application to find the maximum temperature per year in the weather dataset
 * 
 * 
 * For more data information, see the following link. 
 * http://www1.ncdc.noaa.gov/pub/data/noaa/isd-lite/isd-lite-technical-document.pdf
 * 
 * Data set can be downloaded using the command below
 * wget -r -l1 -A.gz http://www1.ncdc.noaa.gov/pub/data/noaa/isd-lite/2013/
 * 
 * The code is inspired by the Book "Hadoop the definitive guide"
 * 
 * 
 * */

import java.text.DecimalFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

	private static final int MISSING = 9999;

	public static void main(String[] args) throws Exception {
		// In the main function, we run the MapReduce job

		/*
		 * A JobConf object forms the specification of the job. It gives you
		 * control over how the job is run.
		 */

		Job job = new Job();

		// setJarByClass identifies the jar containing the MaxTemperature.class
		// This jar file is executed for the MapReduce job.
		// We can explicitly specify the name of the JAR file (after packaging
		// the code into a JAR file)
		// and run the job on a Hadoop cluster.

		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max temperature");

		/*
		 * we specify the input and output paths. An input path is specified by
		 * calling the static addInputPath() method on FileInputFormat, and it
		 * can be a single file, a directory (in which case, the input forms all
		 * the files in that directory), or a file pattern. As the name
		 * suggests, addInputPath() can be called more than once to use input
		 * from multiple paths. The output path (of which there is only one) is
		 * specified by the static setOutput Path() method on FileOutputFormat.
		 * It specifies a directory where the output files from the reducer
		 * functions are written. The directory shouldn’t exist before running
		 * the job, as Hadoop will complain and not run the job. This precaution
		 * is to prevent data loss (it can be very annoying to accidentally
		 * overwrite the output of a long job with another).
		 */

		FileInputFormat
				.addInputPath(
						job,
						new Path(
								"hdfs://quickstart.cloudera:8020/user/projects/maxTemperature/076700-99999-2013"));
		FileInputFormat
				.addInputPath(
						job,
						new Path(
								"hdfs://quickstart.cloudera:8020/user/projects/maxTemperature/010010-99999-2012"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/mnt/partage/NationalClimateDataCenterrecord/output"));

		/*
		 * We specify the map and reduce types to use via the setMapperClass()
		 * and setReducerClass() methods.
		 */

		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);

		/*
		 * The setOutputKeyClass() and setOutputValueClass() methods control the
		 * output types for the map and the reduce functions
		 */

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/* We Submit the job to the cluster and wait for it to finish */
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
