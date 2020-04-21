import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortTotalOrder {

	public static final String PARTITION_PATH = "/user/cpre419/lab3/_partitions";

	public static void main(String[] args) throws Exception {

		// Set the number of reducer (No more than 10)
		int numReducers = 10;

		Configuration configuration = new Configuration();

		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: Sort <in> <out>");
			System.exit(2);
		}

		Path inputPath = new Path(otherArgs[0]);
		Path outputPath = new Path(otherArgs[1]);

		configuration.setInt("Count", 0);

		// Use InputSampler.RandomSampler to create sample list
		// Although the efficiency of RandomSampler is the lowest in all three kinds of
		// Sampler (interval, splitSampler)
		// But randomSampler is the most accurate sampler class for this experiment
		// The argument set as the following:
		// freq: 0.1 (probability with which a key will be chosen)
		// numSamplers: 5000000 (total number of samples to obtain from all splits)
		// maxSplitSampled: 9 (the maximum number of splits to examine)
		RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.15, 5600000);

		// Set the path of partition file
		TotalOrderPartitioner.setPartitionFile(configuration, new Path(PARTITION_PATH));

		Job job = Job.getInstance(configuration, "Exp1");
		job.setJarByClass(SortTotalOrder.class);
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
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// SequenceFileOutputFormat.setCompressOutput(job, true);
		// SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		// SequenceFileOutputFormat.setOutputCompressionType(job,
		// CompressionType.BLOCK);

		// Use TotalOrderPartitioner class to sort input data
		job.setPartitionerClass(TotalOrderPartitioner.class);

		FileInputFormat.addInputPath(job, inputPath);

		// Output path
		FileOutputFormat.setOutputPath(job, outputPath);

		// Write the partition file to the partition path set above
		InputSampler.writePartitionFile(job, sampler);

		URI partitionUri = new URI(PARTITION_PATH);
		job.addCacheFile(partitionUri);

		FileSystem fileSystem = FileSystem.get(configuration);
		fileSystem.delete(outputPath, true); // TODO: Maybe not this.
		if (job.waitForCompletion(true)) {
			// Create path object and check for its existence
			Path partitionPath = new Path(PARTITION_PATH);
			if (fileSystem.exists(partitionPath)) { // false indicates do not deletes recursively
				fileSystem.delete(partitionPath, false);
			}
			System.exit(0);
		}
	}

}