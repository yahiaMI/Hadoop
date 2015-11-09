// MaxTemperatureReducer Reducer for maximum temperature

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	/**
	 * We specify the input and output types. The input types of the reduce
	 * function matches the output type of the map function: Text and
	 * IntWritable The output types of the reduce function are Text and
	 * IntWritable, for a year and its maximum temperature.
	 */

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		/*
		 * For each year, we find the max temperature and we add that to the
		 * OutputCollector
		 */
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new IntWritable(maxValue));
	}
}