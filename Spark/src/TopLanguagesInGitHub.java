import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

/**
 * Takes list of GitHub repositories as input and returns the list of languages
 * with the most starred repository for each language, sorted by number of
 * repositories that use that language.
 * 
 * @author Grayson Cox
 */
public class TopLanguagesInGitHub {

	private final static String APP_NAME = "TopLanguagesInGitHub in Spark";
	private final static String USAGE = "Usage: TopLanguagesInGitHub <input> <output>";

	private final static int NUM_PARTITIONS = 1;

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println(USAGE);
			System.exit(1);
		}

		String inputPath = args[0];
		String outputPath = args[1];

		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
		JavaSparkContext context = new JavaSparkContext(sparkConf);

		// Get lines from file.
		JavaRDD<String> lines = context.textFile(inputPath);

		// Parse lines as GitHubRepositoryTuples.
		JavaRDD<GitHubRepositoryTuple> inputTuples = lines.map(line -> GitHubRepositoryTuple.fromString(line));

		// Convert to LanguageWithTopRepositoryTuples with number of repositories per
		// language being 1.
		JavaRDD<LanguageWithTopRepositoryTuple> languagesWithTopRepositories = inputTuples
				.map(tuple -> new LanguageWithTopRepositoryTuple(tuple.getLanguage(), 1, tuple.getName(),
						tuple.getNumStars()));

		// Use language as the key for reducing.
		JavaPairRDD<String, LanguageWithTopRepositoryTuple> languagePairs = languagesWithTopRepositories
				.mapToPair(tuple -> new Tuple2<String, LanguageWithTopRepositoryTuple>(tuple.getLanguage(), tuple));

		// Combine all LanguageWithTopRepositoryTuple for each key.
		languagePairs = languagePairs.reduceByKey((v1, v2) -> combine(v1, v2), NUM_PARTITIONS);

		// Strip the redundant key from each pair.
		languagesWithTopRepositories = languagePairs.map(pair -> pair._2());

		// Sort by number of repositories.
		languagesWithTopRepositories = languagesWithTopRepositories.sortBy(tuple -> tuple.getNumRepositories(), false,
				NUM_PARTITIONS);

		// Save output, deleting existing directory if needed.
		deleteFile(outputPath, context);
		languagesWithTopRepositories.saveAsTextFile(outputPath);

		context.stop();
		context.close();
	}

	/**
	 * Combines two LanguageWithTopRepositoryTuples, choosing the repository name
	 * and number of stars of that which has the greatest number of stars.
	 * 
	 * @param tuple1: A LanguageWithTopRepositoryTuple.
	 * @param tuple2: Another LanguageWithTopRepositoryTuple.
	 * @return A LanguageWithTopRepositoryTuple with the repository name with the
	 *         greatest number of stars.
	 */
	private static LanguageWithTopRepositoryTuple combine(LanguageWithTopRepositoryTuple tuple1,
			LanguageWithTopRepositoryTuple tuple2) {
		String language = tuple1.getLanguage();
		int numRepositories = tuple1.getNumRepositories() + tuple2.getNumRepositories();
		String repositoryName;
		int numStars;
		if (tuple1.getNumStars() > tuple2.getNumStars()) {
			repositoryName = tuple1.getTopRepository();
			numStars = tuple1.getNumStars();
		} else {
			repositoryName = tuple2.getTopRepository();
			numStars = tuple2.getNumStars();
		}
		return new LanguageWithTopRepositoryTuple(language, numRepositories, repositoryName, numStars);
	}

	/**
	 * Deletes the file at the given path.
	 * 
	 * @param path:    The path of the file to be deleted.
	 * @param context: The JavaSparkContext of the current job.
	 * @throws IOException
	 */
	private static void deleteFile(String path, JavaSparkContext context) throws IOException {
		FileSystem fileSystem = FileSystem.get(context.hadoopConfiguration());
		fileSystem.delete(new Path(path), true);
	}

}

/**
 * Represents a triple of the form "[name] [language] [number of stars]".
 * 
 * @author Grayson Cox
 */
@SuppressWarnings("serial")
class GitHubRepositoryTuple extends Tuple3<String, String, Integer> {

	/**
	 * Constructs a GitHubRepositoryTuple from a string.
	 * 
	 * @param line: A string from GitHub.csv.
	 * @return A GitHubRepositoryTuple.
	 */
	public static GitHubRepositoryTuple fromString(String line) {
		String tokens[] = line.split(",");
		String name = tokens[0];
		String language = tokens[1];
		int numStars = Integer.parseInt(tokens[12]);
		return new GitHubRepositoryTuple(name, language, numStars);
	}

	public GitHubRepositoryTuple(String name, String language, Integer numStars) {
		super(name, language, numStars);
	}

	public String getName() {
		return _1();
	}

	public String getLanguage() {
		return _2();
	}

	public Integer getNumStars() {
		return _3();
	}

}

/**
 * Represents a tuple with four fields of the form "[language] [number of
 * repositories] [top repository] [number of stars of top repository]".
 * 
 * @author Grayson Cox
 */
@SuppressWarnings("serial")
class LanguageWithTopRepositoryTuple extends Tuple4<String, Integer, String, Integer> {

	public LanguageWithTopRepositoryTuple(String language, Integer numRepositories, String topRepository,
			Integer numStars) {
		super(language, numRepositories, topRepository, numStars);
	}

	public String getLanguage() {
		return _1();
	}

	public Integer getNumRepositories() {
		return _2();
	}

	public String getTopRepository() {
		return _3();
	}

	public Integer getNumStars() {
		return _4();
	}

}
