import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortCustomPartitioner {

	public static void main(String[] args) throws Exception {

		// Set the number of reducer (No more than 10)
		int numReducers = 10;

		Configuration configuration = new Configuration();

		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: SortCustomPartitioner <in> <out>");
			System.exit(2);
		}

		Path inputPath = new Path(otherArgs[0]);
		Path outputPath = new Path(otherArgs[1]);

		configuration.setInt("Count", 0);

		Job job = Job.getInstance(configuration, "Exp2");
		job.setJarByClass(SortCustomPartitioner.class);
		job.setNumReduceTasks(numReducers);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);

		// KeyValueTextInputFormat.class is the only one class we can use in this
		// experiment
		// Since TotalOrderPartitioner.class requires input must be <Text, Text>
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Use TotalOrderPartitioner class to sort input data
		job.setPartitionerClass(CustomPartitioner.class);

		FileInputFormat.addInputPath(job, inputPath);

		// Output path
		FileOutputFormat.setOutputPath(job, outputPath);

		FileSystem fileSystem = FileSystem.get(configuration);
		fileSystem.delete(outputPath, true); // TODO: Maybe not this.
		if (job.waitForCompletion(true)) {
			System.exit(0);
		}
	}

	public static class CustomPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			char firstCharacter = key.toString().charAt(0);
			double partition = 0;
			if (firstCharacter >= 'a') {
				partition = firstCharacter - 'a' + 36;
			} else if (firstCharacter >= 'A') {
				partition = firstCharacter - 'A' + 10;
			} else if (firstCharacter >= '0') {
				partition = firstCharacter - '0';
			}
			return (int)(partition / 62.0 * numPartitions);
		}

	}

}