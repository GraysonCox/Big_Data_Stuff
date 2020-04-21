import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple1;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Finds the number of cycles of length two in the given undirected graph.
 * 
 * @author Grayson Cox
 */
public class TrianglesInGraph {

	private final static String APP_NAME = "TrianglesInGraph in Spark";
	private final static String USAGE = "Usage: TrianglesInGraph <input> <output>";
	private final static int NUM_PARTITIONS = 2;
	private final static String VERTEX_DELIMITER = "\t";

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println(USAGE);
			System.exit(1);
		}

		String inputPath = args[0];
		String outputPath = args[1];

		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
		JavaSparkContext context = new JavaSparkContext(sparkConf);

		// Get lines from file.
		JavaRDD<String> inputFileLines = context.textFile(inputPath);

		// Parse lines as ordered pairs.
		JavaRDD<Edge> edges = inputFileLines.map(line -> parseEdge(line, VERTEX_DELIMITER))
				.filter(edge -> (edge != null));

		// Make two records for each edge (i.e., (u, v) and (v, u)).
		edges = edges.flatMap(edge -> {
			Edge reversedEdge = new Edge(edge.getV2(), edge.getV1());
			return Arrays.asList(edge, reversedEdge).iterator();
		}).distinct();

		// Group by starting vertex (constructs an adjacency list).
		JavaPairRDD<Vertex, Iterable<Edge>> adjacentEdgesForVertex = edges.groupBy(edge -> edge.getV1(),
				NUM_PARTITIONS);

		// Find all paths of length two.
		JavaRDD<VertexTriple> vertexTriples = adjacentEdgesForVertex
				.flatMap(vertexWithEdges -> getDistinctPathsOfLengthTwo(vertexWithEdges._2()));

		// Group by starting vertex.
		JavaPairRDD<Vertex, Iterable<VertexTriple>> adjacentTriplesForVertex = vertexTriples
				.groupBy(triple -> triple.getV1(), NUM_PARTITIONS);

		// Join the triples and edges by their starting vertices.
		JavaPairRDD<Vertex, Tuple2<Iterable<VertexTriple>, Iterable<Edge>>> adjacentEdgesAndTriplesForVertex = adjacentTriplesForVertex
				.join(adjacentEdgesForVertex, NUM_PARTITIONS);

		// Count the triangles for each vertex and combine all the counts.
		Long numTriangles = adjacentEdgesAndTriplesForVertex
				.map(largeTuple -> countTriangles(largeTuple._2()._1(), largeTuple._2()._2()))
				.reduce((v1, v2) -> v1 + v2);
		JavaRDD<Long> count = context.parallelize(Arrays.asList(numTriangles));

		// Output the number of triangles, deleting existing directory as needed.
		deleteFile(outputPath, context);
		count.saveAsTextFile(outputPath);

		context.stop();
		context.close();
	}

	/**
	 * Constructs an Edge from a string of the form "[vertex] [vertex]".
	 * 
	 * @param line:      A string of the form "[vertex] [vertex]".
	 * @param delimiter: The string that delimits the vertices in the input string.
	 * @return An Edge or null if the input string is invalid.
	 */
	private static Edge parseEdge(String line, String delimiter) {
		String[] tokens = line.split(delimiter);
		if (tokens.length != 2) {
			return null;
		}
		try {
			Vertex v1 = new Vertex(Long.parseLong(tokens[0]));
			Vertex v2 = new Vertex(Long.parseLong(tokens[1]));
			return new Edge(v1, v2);
		} catch (NumberFormatException e) {
			return null;
		}
	}

	/**
	 * Finds all distinct paths of length two in the list of adjacent edges for the
	 * given node. Each VertexTriple returned will have its vertices sorted in
	 * ascending order.
	 * 
	 * @param adjacentEdges: A list of edges adjacent to the common vertex. The
	 *                       first vertex should be the same for every edge in this
	 *                       list
	 * @return The list of distinct paths of length two that can be constructed with
	 *         the given edges.
	 */
	private static Iterator<VertexTriple> getDistinctPathsOfLengthTwo(Iterable<Edge> adjacentEdges) {
		List<VertexTriple> triples = new ArrayList<VertexTriple>();
		Vertex v1, v2, v3;
		VertexTriple newTriple;
		for (Edge inEdge : adjacentEdges) {
			for (Edge outEdge : adjacentEdges) {
				v1 = inEdge.getV2();
				v2 = inEdge.getV1();
				v3 = outEdge.getV2();
				newTriple = new VertexTriple(v1, v2, v3);
				if (!v1.equals(v3) && isDesiredOrder(newTriple)) {
					triples.add(newTriple);
				}
			}
		}
		return triples.iterator();
	}

	/**
	 * Returns the number of triangles found in the given edges and vertex triples.
	 * The first vertex must be the same for all edges and triples.
	 * 
	 * @param adjacentTriples: Vertex triples beginning at the common vertex.
	 * @param adjacentEdges:   Edges beginning at the common vertex.
	 * @return The number of triangles.
	 */
	private static Long countTriangles(Iterable<VertexTriple> adjacentTriples, Iterable<Edge> adjacentEdges) {
		long count = 0;
		for (VertexTriple triple : adjacentTriples) {
			for (Edge edge : adjacentEdges) {
				if (edge.getV2().equals(triple.getV3())) {
					count++;
					break;
				}
			}
		}
		return count;
	}

	/**
	 * Returns true if the vertices in the given VertexTriple are sorted in
	 * ascending order. This prevents duplicates of the same triple.
	 * 
	 * @param triple: A VertexTriple.
	 * @return True if the vertices in the given VertexTriple are sorted in
	 *         ascending order.
	 */
	private static boolean isDesiredOrder(VertexTriple triple) {
		return (triple.getV1().compareTo(triple.getV2()) < 0) && (triple.getV2().compareTo(triple.getV3()) < 0);
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
		fileSystem.close();
	}

}

/**
 * Represents a vertex in a graph. I used Tuple\<Long\> as the super type
 * because I couldn't find a reasonable way to make a custom Serializable class.
 * 
 * @author Grayson Cox
 */
class Vertex extends Tuple1<Long> implements Comparable<Vertex> {

	private static final long serialVersionUID = -5686949859706033805L;

	public Vertex(long id) {
		super(id);
	}

	public long getId() {
		return _1();
	}

	@Override
	public String toString() {
		return Long.toString(_1());
	}

	public int compareTo(Vertex arg0) {
		return Long.compare(this.getId(), arg0.getId());
	}

}

/**
 * Represents an ordered pair of vertices.
 * 
 * @author Grayson Cox
 */
class Edge extends Tuple2<Vertex, Vertex> implements Comparable<Edge> {

	private static final long serialVersionUID = -4945937848812327635L;

	public Edge(Vertex v1, Vertex v2) {
		super(v1, v2);
	}

	public Vertex getV1() {
		return _1();
	}

	public Vertex getV2() {
		return _2();
	}

	public int compareTo(Edge o) {
		return Comparator.comparing(Edge::getV1).thenComparing(Edge::getV2).compare(this, o);
	}

}

/**
 * Represents a triple of vertices.
 * 
 * @author Grayson Cox
 */
class VertexTriple extends Tuple3<Vertex, Vertex, Vertex> implements Comparable<VertexTriple> {

	private static final long serialVersionUID = -6395277080904764605L;

	public VertexTriple(Vertex v1, Vertex v2, Vertex v3) {
		super(v1, v2, v3);
	}

	public Vertex getV1() {
		return _1();
	}

	public Vertex getV2() {
		return _2();
	}

	public Vertex getV3() {
		return _3();
	}

	public int compareTo(VertexTriple o) {
		return Comparator.comparing(VertexTriple::getV1).thenComparing(VertexTriple::getV2)
				.thenComparing(VertexTriple::getV3).compare(this, o);
	}

}
