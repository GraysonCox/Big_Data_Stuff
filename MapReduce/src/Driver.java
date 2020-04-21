import java.io.IOException;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * A MapReduce program that finds the top 10 most frequently occurring bigrams
 * in a given file.
 * 
 * @author Grayson Cox
 *
 */
public class Driver {

	public static void main(String[] args) throws Exception {

		// The number of reduce tasks
		int numReducersRound1 = 4;
		int numReducersRound2 = 1; // There should be only one reducer in the second round.

		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: Driver <in> <temp> <out>");
			System.exit(2);
		}

		String input = otherArgs[0];
		String temp = otherArgs[1];
		String output = otherArgs[2];

		// Create job for round 1
		Job job1 = Job.getInstance(conf, "Driver Program Round 1");

		// Attach the job to this Driver
		job1.setJarByClass(Driver.class);

		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job1.setNumReduceTasks(numReducersRound1);

		// The datatype of the mapper output Key, Value
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);

		// The datatype of the reducer output Key, Value
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		// The class that provides the map method
		job1.setMapperClass(MapOne.class);

		// The class that provides the reduce method
		job1.setReducerClass(ReduceOne.class);

		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job1.setInputFormatClass(TextInputFormat.class);

		// Decides the Output Format
		job1.setOutputFormatClass(TextOutputFormat.class);

		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job1, new Path(input));

		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job1, new Path(temp));

		// Run the job
		job1.waitForCompletion(true);

		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1

		Job job2 = Job.getInstance(conf, "Driver Program Round 2");
		job2.setJarByClass(Driver.class);
		job2.setNumReduceTasks(numReducersRound2);

		// Should be match with the output datatype of mapper and reducer
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job2.setMapperClass(MapTwo.class);
		job2.setReducerClass(ReduceTwo.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job2, new Path(temp));
		FileOutputFormat.setOutputPath(job2, new Path(output));

		// Run the job
		job2.waitForCompletion(true);
	}

	/**
	 * Mapper for first MapReduce round. Maps a line of text to a list of bigram.
	 * Emits the bigram itself as the key and the number '1' as the value,
	 * indicating that the bigram was found one time.
	 */
	public static class MapOne extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final char DELIMITER = ' ';

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().toLowerCase();

			StringTokenizer tokens = new StringTokenizer(line);

			String previousToken = null, currentToken;
			Text bigram;
			IntWritable numOccurrences = new IntWritable(1);

			while (tokens.hasMoreTokens()) {
				currentToken = tokens.nextToken();
				if (isWord(currentToken)) {
					currentToken = stripPunctuation(currentToken);
					if (previousToken != null) {
						bigram = new Text(previousToken + DELIMITER + currentToken);
						context.write(bigram, numOccurrences);
					}
					previousToken = currentToken;

				} else if (isEndOfSentence(currentToken)) {
					previousToken = null;
				}
			}
		}

		/**
		 * Indicates whether the given token ends with a '.', '?', or '!'.
		 * 
		 * @param token A String.
		 * @return True if the given String ends with a '.', '?', or '!'.
		 */
		private boolean isEndOfSentence(String token) {
			if (token == null || token.isEmpty())
				return false;
			char lastChar = token.charAt(token.length() - 1);
			return (lastChar == '.') || (lastChar == '?') || (lastChar == '!');
		}

		/**
		 * Indicates whether the given token contains alphabetic characters.
		 * 
		 * @param token A String.
		 * @return True if the given String contains alphabetic characters.
		 */
		private boolean isWord(String token) {
			if (token == null)
				return false;
			return token.matches(".*[a-zA-Z].*");
		}

		/**
		 * Removes all non-alphanumeric characters from the string.
		 * 
		 * @param token A String.
		 * @return A string containing only alphanumeric characters.
		 */
		private String stripPunctuation(String token) {
			return token.replaceAll("[^a-zA-Z0-9]", "");
		}

	}

	/**
	 * Reducer for first MapReduce round. The input key is the bigram in plain text,
	 * and the input value is the number of occurrences of the bigram. This reducer
	 * counts all the occurrences for the given bigram and emits the bigram name as
	 * the key and the number of occurrences as the value.
	 */
	public static class ReduceOne extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}

	}

	/**
	 * Mapper for second MapReduce round. Reads in several bigrams and their
	 * respective numbers of occurrences and emits the ten most frequently occurring
	 * bigrams that it encounters.
	 */
	public static class MapTwo extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final int NUM_BIGRAMS_TO_RETURN = 10;

		private PriorityQueue<Entry> topBigramQueue;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			topBigramQueue = new PriorityQueue<Entry>(NUM_BIGRAMS_TO_RETURN + 1);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			String bigramName = tokens[0];
			int numOccurrences = Integer.parseInt(tokens[1]);
			topBigramQueue.add(new Entry(numOccurrences, bigramName));
			if (topBigramQueue.size() > NUM_BIGRAMS_TO_RETURN) {
				topBigramQueue.poll();
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			Entry currentEntry;
			int numOccurrences;
			String bigramName;
			while (!topBigramQueue.isEmpty()) {
				currentEntry = topBigramQueue.poll();
				numOccurrences = currentEntry.getPriority();
				bigramName = currentEntry.getValue();
				context.write(new Text(bigramName), new IntWritable(numOccurrences));
			}
		}

	}

	/**
	 * Reducer for second MapReduce round. This functions the same as the Mapper; it
	 * emits the ten most frequently occurring bigrams. In order to find the global
	 * top ten bigrams, there must be only one instance of this reducer.
	 */
	public static class ReduceTwo extends Reducer<Text, IntWritable, Text, IntWritable> {

		private final int NUM_BIGRAMS_TO_RETURN = 10;

		private PriorityQueue<Entry> topBigramQueue;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			topBigramQueue = new PriorityQueue<Entry>(NUM_BIGRAMS_TO_RETURN + 1);
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			String bigramName = key.toString();
			int numOccurrences = 0;
			for (IntWritable value : values) {
				numOccurrences += value.get();
			}
			topBigramQueue.add(new Entry(numOccurrences, bigramName));
			if (topBigramQueue.size() > NUM_BIGRAMS_TO_RETURN) {
				topBigramQueue.poll();
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			Entry currentEntry;
			int numOccurrences;
			String bigramName;
			while (!topBigramQueue.isEmpty()) {
				currentEntry = topBigramQueue.poll();
				numOccurrences = currentEntry.getPriority();
				bigramName = currentEntry.getValue();
				context.write(new Text(bigramName), new IntWritable(numOccurrences));
			}
		}

	}

}

/**
 * An entity representing a String with a integral priority value to be used in
 * a priority queue.
 */
class Entry implements Comparable<Entry> {

	private int priority;
	private String value;

	public Entry(int priority, String value) {
		this.priority = priority;
		this.value = value;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public int compareTo(Entry o) {
		return Integer.compare(this.getPriority(), o.getPriority());
	}

}
