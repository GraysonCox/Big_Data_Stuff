import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

/**
 * @author Grayson Cox
 */
public class RegenerateLogFile {

	private final static String APP_NAME = "RegenerateLogFile in Spark";
	private final static String USAGE = "RegenerateLogFile <ip_trace_file> <blocked_connection_ids_file> <firewall_log_output> <top_source_ip_output>";

	private final static int NUM_REDUCERS = 1;

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println(USAGE);
			System.exit(1);
		}

		String ipTraceFile = args[0];
		String blockedConnectionIdFile = args[1];
		String firewallLogOutputPath = args[2];
		String topSourceIpOutputPath = args[3];

		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
		JavaSparkContext context = new JavaSparkContext(sparkConf);

		// Load data from the input files.
		JavaRDD<String> ipTraceLines = context.textFile(ipTraceFile);
		JavaRDD<String> blockedConnectionIdLines = context.textFile(blockedConnectionIdFile);

		// Parse rows using defined schemas.
		JavaRDD<IPTraceTuple> ipTraceTuples = ipTraceLines.map(line -> IPTraceTuple.fromString(line));
		JavaRDD<BlockedConnectionIDTuple> blockedConnectionIdTuples = blockedConnectionIdLines
				.map(line -> BlockedConnectionIDTuple.fromString(line));

		// Extract the connectionId as the key in a JavaPairRDD.
		JavaPairRDD<Integer, IPTraceTuple> ipTracePairs = ipTraceTuples
				.mapToPair(tuple -> new Tuple2<Integer, IPTraceTuple>(tuple.getConnectionId(), tuple));
		JavaPairRDD<Integer, BlockedConnectionIDTuple> blockedConnectionIdPairs = blockedConnectionIdTuples
				.mapToPair(tuple -> new Tuple2<Integer, BlockedConnectionIDTuple>(tuple.getConnectionId(), tuple));

		// Inner join ipTracePairs and blockedConnectionIdPairs and then use the
		// resulting tuples to construct FirewallLogTuples.
		JavaRDD<FirewallLogTuple> firewallLogTuples = ipTracePairs.join(blockedConnectionIdPairs)
				.map(tuple -> makeFirewallLogTuple(tuple._2()._1(), tuple._2()._2()));

		// Remove non-blocked records.
		firewallLogTuples = firewallLogTuples.filter(tuple -> tuple.getAction().equalsIgnoreCase("Blocked"));

		// Sort by time.
		firewallLogTuples = firewallLogTuples.sortBy(tuple -> tuple.getTime(), true, NUM_REDUCERS);

		// Save the results.
		saveAsTextFile(firewallLogTuples, firewallLogOutputPath);

		// Pair each sourceIp with value 1 and then calculate the some of values for
		// each unique sourceIp.
		JavaPairRDD<String, Integer> occurrencesPerSourceIp = firewallLogTuples
				.mapToPair(tuple -> new Tuple2<String, Integer>(tuple.getSourceIp(), 1))
				.reduceByKey((v1, v2) -> v1 + v2, NUM_REDUCERS);

		// Swap the keys and values, sort by key, and then swap keys and values again.
		occurrencesPerSourceIp = occurrencesPerSourceIp.mapToPair(tuple -> swapKeyAndValue(tuple)).sortByKey(false)
				.mapToPair(tuple -> swapKeyAndValue(tuple));

		// Save the results.
		saveAsTextFile(occurrencesPerSourceIp, topSourceIpOutputPath);

		context.stop();
		context.close();
	}

	/**
	 * Constructs a FirewallLogTuple from the given IPTraceTuple and
	 * BlockedConnectionIDTuple.
	 * 
	 * @param ipTraceTuple:             An IPTraceTuple.
	 * @param blockedConnectionIdTuple: A BlockedConnectionIDTuple.
	 * @return A FirewallLogTuple.
	 */
	private static FirewallLogTuple makeFirewallLogTuple(IPTraceTuple ipTraceTuple,
			BlockedConnectionIDTuple blockedConnectionIdTuple) {
		return new FirewallLogTuple(ipTraceTuple.getTime(), ipTraceTuple.getConnectionId(), ipTraceTuple.getSourceIp(),
				ipTraceTuple.getDestinationIp(), blockedConnectionIdTuple.getAction());
	}

	/**
	 * Returns the given key-value pair, in which the key and value are swapped.
	 * 
	 * @param <K>:   The data type of the original key.
	 * @param <V>:   The data type of the original value.
	 * @param tuple: A key-value pair.
	 * @return The given key-value pair, in which the key and value are swapped.
	 */
	private static <K, V> Tuple2<V, K> swapKeyAndValue(Tuple2<K, V> tuple) {
		return new Tuple2<V, K>(tuple._2(), tuple._1());
	}

	/**
	 * Saves the given data to a file at the given path in HDFS. If the file already
	 * exists, it will be deleted.
	 * 
	 * @param data: The data to be written to a file.
	 * @param path: The file path to be written to.
	 * @throws IOException
	 */
	private static void saveAsTextFile(JavaRDDLike data, String path) throws IOException {
		FileSystem fileSystem = FileSystem.get(data.context().hadoopConfiguration());
		fileSystem.delete(new Path(path), true);
		data.saveAsTextFile(path);
	}

}

/**
 * Represents a line from an IP trace file. Note that this does not include the
 * protocol field or anything after it.
 * 
 * @author Grayson Cox
 */
class IPTraceTuple extends Tuple4<String, Integer, String, String> {

	/**
	 * Constructs an IPTraceTuple from a line from an IP trace file.
	 * 
	 * @param line: A string of the form "[time] [connection ID] [source IP] >
	 *              [destination IP] ...".
	 * @return An IPTraceTuple.
	 */
	public static IPTraceTuple fromString(String line) {
		String tokens[] = line.split(" ");
		String time = tokens[0];
		int connectionId = Integer.parseInt(tokens[1]);
		String sourceIp = tokens[2];
		String destinationIp = tokens[4];
		return new IPTraceTuple(time, connectionId, sourceIp, destinationIp);
	}

	public IPTraceTuple(String time, Integer connectionId, String sourceIp, String destinationIp) {
		super(time, connectionId, sourceIp, destinationIp);
	}

	public String getTime() {
		return _1();
	}

	public Integer getConnectionId() {
		return _2();
	}

	public String getSourceIp() {
		return _3();
	}

	public String getDestinationIp() {
		return _4();
	}

}

/**
 * Represents a line in the "raw_block" file, I guess.
 * 
 * @author Grayson Cox
 */
class BlockedConnectionIDTuple extends Tuple2<Integer, String> {

	/**
	 * Constructs a BlockedConnectionIDTuple from a string with the form
	 * "[connection ID] Blocked".
	 * 
	 * @param line: A string of the form "[connection ID] Blocked".
	 * @return A BlockedConnectionIDTuple.
	 */
	public static BlockedConnectionIDTuple fromString(String line) {
		String tokens[] = line.split(" ");
		int connectionId = Integer.parseInt(tokens[0]);
		String action = tokens[1];
		return new BlockedConnectionIDTuple(connectionId, action);
	}

	public BlockedConnectionIDTuple(Integer connectionId, String action) {
		super(connectionId, action);
	}

	public Integer getConnectionId() {
		return _1();
	}

	public String getAction() {
		return _2();
	}

}

/**
 * Represents a line in a firewall log file.
 * 
 * @author Grayson Cox
 */
class FirewallLogTuple extends Tuple5<String, Integer, String, String, String> {

	public FirewallLogTuple(String time, Integer connectionId, String sourceIp, String destinationIp, String action) {
		super(time, connectionId, sourceIp, destinationIp, action);
	}

	public String getTime() {
		return _1();
	}

	public Integer getConnectionId() {
		return _2();
	}

	public String getSourceIp() {
		return _3();
	}

	public String getDestinationIp() {
		return _4();
	}

	public String getAction() {
		return _5();
	}

}
