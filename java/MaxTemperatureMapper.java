// MaxTemperatureMapper Mapper for maximum temperature

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	/*
	 * The Mapper interface is a generic type, with four formal type parameters
	 * that specify the input key (line number, input value (line of text),
	 * output key (year), and output value (air temperature) types of the map
	 * function.
	 * 
	 * Hadoop provides its own set of basic types that are optimized for network
	 * serialization. Here we use LongWritable, which corresponds to a Java
	 * Long, Text (like Java String), and IntWritable (like Java Integer)
	 */

	private static final int MISSING = 9999;

	@Override
	// Map method declaration
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/*
		 * We convert the Text value containing the line of input into a Java
		 * String
		 */
		String line = value.toString();

		/*
		 * We extract the columns we are interested in, below the year, and the air
		 * temperature
		 */
		String year = line.substring(0, 4);

		int airTemperature = Integer.parseInt(line.substring(13, 19)
				.replaceAll("\\s+", ""));

		if (airTemperature != MISSING) {

			/*
			 * The map() method also provides an instance of OutputCollector to
			 * write the output to. In this case, we write the year as a Text
			 * object (since we are just using it as a key), and the temperature
			 * wrapped in an IntWritable.
			 */

			context.write(new Text(year), new IntWritable(airTemperature));

		}

	}
}