package bt7s7k7.vinf_project.spark;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.ListUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import bt7s7k7.vinf_project.common.Logger;
import bt7s7k7.vinf_project.indexing.DocumentDatabase;

public final class SparkTest {
	public final DocumentDatabase documentDatabase;

	public SparkTest(DocumentDatabase documentDatabase) {
		this.documentDatabase = documentDatabase;
	}

	private static final Pattern CHARACTERS_TO_REMOVE = Pattern.compile("[^a-z0-9]");

	private static String normalizeNameForLookup(String name) {
		return CHARACTERS_TO_REMOVE.matcher(name.toLowerCase()).replaceAll("");
	}

	public void run() {
		Logger.info("Preparing lookup from document database...");

		// Prepare a list of existing documents with only alphanumeric characters for search
		var lookup = this.documentDatabase.stream()
				// Get document name
				.map(Map.Entry::getValue)
				// Transform document name and create a lookup table for the original names
				.collect(Collectors.toMap(SparkTest::normalizeNameForLookup, List::of, ListUtils::union));

		Logger.info("Starting spark session...");

		// Start spark session
		var spark = SparkSession.builder()
				.config("spark.master", "local")
				.config("spark.ui.showConsoleProgress", true)
				.getOrCreate();

		// Register the normalization function
		spark.udf().register("normalizeNameForLookup", (UDF1<String, String>) SparkTest::normalizeNameForLookup, DataTypes.StringType);

		Logger.success("Started, " + spark.version());

		var start = Instant.now();

		Logger.info("Reading...");

		// Read the input XML file
		var df = spark.read()
				.option("rowTag", "page")
				.option("inferSchema", false)
				.schema(WikipediaSchema.FULL_SCHEMA)
				.xml("/mnt/solid/Code/Skola/VINF/VINF-Project/project/external/enwiki-20251020-pages-articles-multistream1.xml-p1p41242");

		Logger.info("Processing...");

		// Find rows with relevant titles
		var relevant = df
				.withColumn("normalized-title", callUDF("normalizeNameForLookup", col("title")))
				.filter(col("normalized-title").isin(lookup.keySet().stream().toArray()));

		// Show results
		relevant.show(10000);

		var end = Instant.now();
		Logger.success("Done. Took: " + Duration.between(start, end));

		spark.stop();
	}
}
