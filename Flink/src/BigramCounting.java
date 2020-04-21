import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * Prints the top 10 bigrams for each 1000 words in the input stream.
 * 
 * @author Grayson Cox
 */
public class BigramCounting {

	private static final String INPUT_FILE = "/home/cpre419/Downloads/shakespeare";

	private static final int WINDOW_SIZE = 1000;
	private static final int K = 10;

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.getConfig().setGlobalJobParameters(params);

		DataStream<String> inputLines = environment.readTextFile(INPUT_FILE);

		DataStream<TaggedValue<BigramFrequencyPair>> bigramFrequencyPairsPerWindow = inputLines
				.flatMap(new SplitWordsFlatMapFunction()).countWindowAll(WINDOW_SIZE)
				.process(new CreateBigramFrequencyPairsProcessAllWindowFunction());

		bigramFrequencyPairsPerWindow.addSink(new PrintTopKBigramsPerWindowRichSinkFunction(K));

		environment.execute("Streaming WordCount Example");
	}

	/**
	 * Separates all the words in each string in the DataStream.
	 * 
	 * @author Grayson Cox
	 */
	private static class SplitWordsFlatMapFunction implements FlatMapFunction<String, String> {

		private static final long serialVersionUID = 3568132244519979597L;

		private static final String DELIMITER = " ";

		@Override
		public void flatMap(String value, Collector<String> out) throws Exception {
			String[] tokens = value.split(DELIMITER);
			for (String token : tokens) {
				out.collect(token);
			}
		}

	}

	/**
	 * Finds all bigrams in each window and counts the frequencies of each bigram in
	 * each window.
	 * 
	 * @author Grayson Cox
	 */
	private static class CreateBigramFrequencyPairsProcessAllWindowFunction
			extends ProcessAllWindowFunction<String, TaggedValue<BigramFrequencyPair>, GlobalWindow> {

		private static final long serialVersionUID = -6280560112493441343L;

		private int latestWindowId = 0;

		@Override
		public void process(
				ProcessAllWindowFunction<String, TaggedValue<BigramFrequencyPair>, GlobalWindow>.Context context,
				Iterable<String> input, Collector<TaggedValue<BigramFrequencyPair>> out) throws Exception {
			Map<Bigram, Integer> bigramsWithFrequencies = getBigramsWithFrequencies(input);
			int currentWindowId = latestWindowId++;
			for (Map.Entry<Bigram, Integer> mapEntry : bigramsWithFrequencies.entrySet()) {
				Bigram bigram = mapEntry.getKey();
				int frequency = mapEntry.getValue();
				BigramFrequencyPair bigramFrequencyPair = new BigramFrequencyPair(bigram, frequency);
				out.collect(new TaggedValue<BigramFrequencyPair>(currentWindowId, bigramFrequencyPair));
			}
		}

		/**
		 * Returns a Map in which the keys are Bigrams and the values are their
		 * respective frequencies.
		 * 
		 * @param words The words to create bigrams with.
		 * @return A Map in which the keys are Bigrams and the values are their
		 *         respective frequencies.
		 */
		private Map<Bigram, Integer> getBigramsWithFrequencies(Iterable<String> words) {
			Map<Bigram, Integer> distinctBigramsWithFrequencies = new HashMap<Bigram, Integer>();
			String previousToken = null;
			for (String currentToken : words) {
				currentToken = currentToken.toLowerCase();
				if (isWord(currentToken)) {
					currentToken = stripPunctuation(currentToken);
					if (previousToken != null) {
						Bigram newBigram = new Bigram(previousToken, currentToken);
						int newFrequency = distinctBigramsWithFrequencies.getOrDefault(newBigram, 0) + 1;
						distinctBigramsWithFrequencies.put(newBigram, newFrequency);
					}
					previousToken = currentToken;
				} else if (isEndOfSentence(currentToken)) {
					previousToken = null;
				}
			}
			return distinctBigramsWithFrequencies;
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
	 * Prints the k bigrams with highest frequency for each window.
	 * 
	 * @author Grayson Cox
	 */
	private static class PrintTopKBigramsPerWindowRichSinkFunction
			extends RichSinkFunction<TaggedValue<BigramFrequencyPair>> {

		private static final long serialVersionUID = -7233351789082527329L;

		private static final Comparator<BigramFrequencyPair> TREESET_COMPARATOR = Comparator
				.comparing(BigramFrequencyPair::getFrequency).reversed().thenComparing(BigramFrequencyPair::getBigram);

		private int k;
		private List<TreeSet<BigramFrequencyPair>> topBigramsPerWindow;

		/**
		 * Constructs a new PrintTopKBigramsPerWindowRichSinkFunction.
		 * 
		 * @param k The number of bigrams to print for each window.
		 */
		public PrintTopKBigramsPerWindowRichSinkFunction(int k) {
			super();
			this.k = k;
			this.topBigramsPerWindow = new ArrayList<TreeSet<BigramFrequencyPair>>();
		}

		@Override
		public void invoke(TaggedValue<BigramFrequencyPair> value) throws Exception {
			TreeSet<BigramFrequencyPair> topBigrams = getTreeSetForWindow(value.f0);
			topBigrams.add(value.f1);
			if (topBigrams.size() > k) {
				topBigrams.pollLast();
			}
		}

		@Override
		public void close() throws IOException {
			BigramFrequencyPair currentBigram;
			for (int i = 0; i < topBigramsPerWindow.size(); i++) {
				TreeSet<BigramFrequencyPair> topBigrams = topBigramsPerWindow.get(i);
				printRowHeader(k, i);
				while (!topBigrams.isEmpty()) {
					currentBigram = topBigrams.pollFirst();
					printBigramFrequencyPair(currentBigram);
				}
			}
		}

		/**
		 * Returns the priority queue containing the top K elements from the specified
		 * window.
		 * 
		 * @param windowNum The number identifying the window.
		 * @return The priority queue containing the top K elements from the specified
		 *         window.
		 */
		private TreeSet<BigramFrequencyPair> getTreeSetForWindow(int windowNum) {
			TreeSet<BigramFrequencyPair> topBigrams;
			if (topBigramsPerWindow.size() > windowNum) {
				topBigrams = topBigramsPerWindow.get(windowNum);
			} else {
				topBigrams = new TreeSet<BigramFrequencyPair>(TREESET_COMPARATOR);
				topBigramsPerWindow.add(windowNum, topBigrams);
			}
			return topBigrams;
		}

		/**
		 * Prints a header indicating the window number to the console.
		 * 
		 * @param k         The number of records to show for each window.
		 * @param windowNum The number of the window.
		 */
		private void printRowHeader(int k, int windowNum) {
			System.out.printf("Top %d bigrams in window %d:\n", k, windowNum);
		}

		/**
		 * Prints a BigramFrequencyPair to the console.
		 * 
		 * @param bigramFrequencyPair The thing to print.
		 */
		private void printBigramFrequencyPair(BigramFrequencyPair bigramFrequencyPair) {
			System.out.println(bigramFrequencyPair);
		}

	}

	/**
	 * Represents a bigram as an ordered pair of words.
	 * 
	 * @author Grayson Cox
	 */
	public static class Bigram extends Tuple2<String, String> implements Comparable<Bigram> {

		private static final long serialVersionUID = -227693736759206632L;

		public Bigram() {
			super();
		}

		public Bigram(String word1, String word2) {
			super(word1, word2);
		}

		public String getWord1() {
			return f0;
		}

		public String getWord2() {
			return f1;
		}

		@Override
		public int compareTo(Bigram bigram) {
			return Comparator.comparing(Bigram::getWord1).thenComparing(Bigram::getWord2).compare(this, bigram);
		}

		@Override
		public Bigram copy() {
			return new Bigram(f0, f1);
		}

	}

	/**
	 * Represents a triple in which the first column is a bigram and the third
	 * column is the integral frequency of the bigram.
	 * 
	 * @author Grayson Cox
	 */
	public static class BigramFrequencyPair extends Tuple2<Bigram, Integer> {

		private static final long serialVersionUID = 8084965628419587033L;

		public BigramFrequencyPair() {
			super();
		}

		public BigramFrequencyPair(String word1, String word2, Integer frequency) {
			super(new Bigram(word1, word2), frequency);
		}

		public BigramFrequencyPair(Bigram bigram, Integer frequency) {
			super(bigram, frequency);
		}

		public Bigram getBigram() {
			return f0;
		}

		public Integer getFrequency() {
			return f1;
		}

		@Override
		public BigramFrequencyPair copy() {
			return new BigramFrequencyPair(f0.getWord1(), f0.getWord2(), f1);
		}

	}

	/**
	 * Represents a key-value pair with an integral key, and any value type. I use
	 * this to group BigramFrequencyPairs by their window.
	 * 
	 * @author Grayson Cox
	 *
	 * @param <T> The value type.
	 */
	public static class TaggedValue<T> extends Tuple2<Integer, T> {

		private static final long serialVersionUID = -7580491800298820362L;

		public TaggedValue() {
			super();
		}

		public TaggedValue(int tag, T value) {
			super(tag, value);
		}

		public Integer getTag() {
			return f0;
		}

		public T getValue() {
			return f1;
		}

	}

}
