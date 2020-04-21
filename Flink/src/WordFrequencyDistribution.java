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
 * Counts the frequency of each distinct word in a given input stream.
 * 
 * @author Grayson Cox
 */
public class WordFrequencyDistribution {

	private static final String INPUT_FILE = "/home/cpre419/Downloads/shakespeare";

	private static final int WINDOW_SIZE = 1000;
	private static final int K = 10;

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.getConfig().setGlobalJobParameters(params);

		DataStream<String> inputLines = environment.readTextFile(INPUT_FILE);

		DataStream<TaggedValue<WordFrequencyPair>> wordFrequencyPairsPerWindow = inputLines
				.flatMap(new CreateWordFrequencyPairsFlatMapFunction()).countWindowAll(WINDOW_SIZE)
				.process(new CombineFrequenciesPerWordProcessAllWindowFunction());

		wordFrequencyPairsPerWindow.addSink(new PrintTopKWordsPerWindowRichSinkFunction(K));

		environment.execute("Streaming WordCount Example");
	}

	/**
	 * Converts each word to a WordFrequencyPair with the frequency 1.
	 * 
	 * @author Grayson Cox
	 */
	private static class CreateWordFrequencyPairsFlatMapFunction implements FlatMapFunction<String, WordFrequencyPair> {

		private static final long serialVersionUID = -8725017016130354824L;

		@Override
		public void flatMap(String value, Collector<WordFrequencyPair> out) {
			String[] tokens = value.toLowerCase().split("\\W+");
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new WordFrequencyPair(token, 1));
				}
			}
		}

	}

	/**
	 * Converts each WordFrequencyPair into an ordered pair consisting of the
	 * WordFrequencyPair's window number and the WordFrequencyPair itself and
	 * combines the frequencies of each word in each window.
	 * 
	 * @author Grayson Cox
	 */
	private static class CombineFrequenciesPerWordProcessAllWindowFunction
			extends ProcessAllWindowFunction<WordFrequencyPair, TaggedValue<WordFrequencyPair>, GlobalWindow> {

		private static final long serialVersionUID = -3763612467003041468L;

		private int windowNum = 0;

		@Override
		public void process(
				ProcessAllWindowFunction<WordFrequencyPair, TaggedValue<WordFrequencyPair>, GlobalWindow>.Context context,
				Iterable<WordFrequencyPair> input, Collector<TaggedValue<WordFrequencyPair>> out) throws Exception {
			int windowNum = this.windowNum++;
			Map<String, Integer> distinctWords = new HashMap<String, Integer>();
			int newFrequency;
			for (WordFrequencyPair value : input) {
				newFrequency = distinctWords.getOrDefault(value.getWord(), 0) + 1;
				distinctWords.put(value.getWord(), newFrequency);
			}
			WordFrequencyPair newPair;
			for (Map.Entry<String, Integer> mapEntry : distinctWords.entrySet()) {
				newPair = new WordFrequencyPair(mapEntry.getKey(), mapEntry.getValue());
				out.collect(new TaggedValue<WordFrequencyPair>(windowNum, newPair));
			}
			distinctWords.clear();
		}

	}

	/**
	 * Prints the k words with highest frequency for each window.
	 * 
	 * @author Grayson Cox
	 */
	private static class PrintTopKWordsPerWindowRichSinkFunction
			extends RichSinkFunction<TaggedValue<WordFrequencyPair>> {

		private static final long serialVersionUID = 2241506719613889875L;

		private static final Comparator<WordFrequencyPair> TREESET_COMPARATOR = Comparator
				.comparing(WordFrequencyPair::getFrequency).reversed().thenComparing(WordFrequencyPair::getWord);

		private int k;
		private List<TreeSet<WordFrequencyPair>> topWordsPerWindow;

		public PrintTopKWordsPerWindowRichSinkFunction(int k) {
			super();
			this.k = k;
			this.topWordsPerWindow = new ArrayList<TreeSet<WordFrequencyPair>>();
		}

		@Override
		public void invoke(TaggedValue<WordFrequencyPair> taggedValue) throws Exception {
			TreeSet<WordFrequencyPair> topWords = getTreeSetForWindow(taggedValue.getTag());
			topWords.add(taggedValue.getValue());
			if (topWords.size() > k) {
				topWords.pollLast();
			}
		}

		@Override
		public void close() throws IOException {
			WordFrequencyPair currentWord;
			for (int i = 0; i < topWordsPerWindow.size(); i++) {
				TreeSet<WordFrequencyPair> topWords = topWordsPerWindow.get(i);
				System.out.printf("Top %d words in window %d:\n", k, i);
				while (!topWords.isEmpty()) {
					currentWord = topWords.pollFirst();
					System.out.println(currentWord);
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
		private TreeSet<WordFrequencyPair> getTreeSetForWindow(int windowNum) {
			TreeSet<WordFrequencyPair> topWords;
			if (topWordsPerWindow.size() > windowNum) {
				topWords = topWordsPerWindow.get(windowNum);
			} else {
				topWords = new TreeSet<WordFrequencyPair>(TREESET_COMPARATOR);
				topWordsPerWindow.add(windowNum, topWords);
			}
			return topWords;
		}

	}

	/**
	 * Represents a key-value pair in which the key is a word and the value is the
	 * integral frequency of the word.
	 * 
	 * @author Grayson Cox
	 */
	public static class WordFrequencyPair extends Tuple2<String, Integer> {

		private static final long serialVersionUID = -5317178812906743290L;

		public WordFrequencyPair() {
			super();
		}

		public WordFrequencyPair(String word, Integer frequency) {
			super(word, frequency);
		}

		public String getWord() {
			return f0;
		}

		public Integer getFrequency() {
			return f1;
		}

		@Override
		public WordFrequencyPair copy() {
			return new WordFrequencyPair(f0, f1);
		}

	}

	/**
	 * Represents a key-value pair with an integral key, and any value type. I use
	 * this to group WordFrequencyPairs by their window.
	 * 
	 * @author Grayson Cox
	 *
	 * @param <T> The value type.
	 */
	public static class TaggedValue<T> extends Tuple2<Integer, T> {

		private static final long serialVersionUID = -3174609034622095367L;

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
