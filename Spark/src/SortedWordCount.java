import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * CprE 419 - Lab 5, Experiment 1
 * 
 * Spark program that counts the number of occurrences of words in a file and
 * outputs the counts in descending order of total occurrences.
 * 
 * @author Grayson Cox
 */
public class SortedWordCount {

	private final static String APP_NAME = "SortedWordCount in Spark";
	private final static String USAGE = "SortedWordCount <input> <output>";

	private final static int NUM_REDUCERS = 2;

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println(USAGE);
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
		JavaSparkContext context = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = context.textFile(args[0]);

		JavaRDD<String> words = getWords(lines);

		JavaPairRDD<String, Integer> ones = pairWithOnes(words);

		JavaPairRDD<String, Integer> counts = addValues(ones, NUM_REDUCERS);

		JavaPairRDD<Integer, String> countAsKey = swapKeyAndValue(counts);

		countAsKey = countAsKey.sortByKey(false);

		countAsKey.saveAsTextFile(args[1]);
		context.stop();
		context.close();
	}

	/**
	 * Separates and returns all words in the given strings.
	 * 
	 * @param lines: Some strings.
	 * @return All words from the given strings.
	 */
	@SuppressWarnings("serial")
	private static JavaRDD<String> getWords(JavaRDD<String> lines) { // TODO: Maybe remove punctuation?
		return lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) {
				return Arrays.asList(s.split("\\s+")).iterator();
			}
		});
	}

	/**
	 * Creates a key-value pair for each string, where the key is the string itself
	 * and the value is one.
	 * 
	 * @param words: Some strings.
	 * @return Key-value pairs in which the keys are the given strings and every
	 *         value is one.
	 */
	@SuppressWarnings("serial")
	private static JavaPairRDD<String, Integer> pairWithOnes(JavaRDD<String> words) {
		return words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
	}

	/**
	 * Calculates the sum of the values for each key.
	 * 
	 * @param pairs:       Some key-value pairs.
	 * @param numReducers: The number of reducers to be used.
	 * @return Key-value pairs where each value is the sum of values for the key.
	 */
	@SuppressWarnings("serial")
	private static JavaPairRDD<String, Integer> addValues(JavaPairRDD<String, Integer> pairs, int numReducers) {
		return pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, numReducers);
	}

	/**
	 * Swaps the key and value for every key-value pair.
	 * 
	 * @param pairs: Some key-value pairs.
	 * @return The given key-value pairs with the keys and values swapped.
	 */
	@SuppressWarnings("serial")
	private static JavaPairRDD<Integer, String> swapKeyAndValue(JavaPairRDD<String, Integer> pairs) {
		return pairs.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
				return new Tuple2<Integer, String>(t._2, t._1);
			}
		});
	}

}
