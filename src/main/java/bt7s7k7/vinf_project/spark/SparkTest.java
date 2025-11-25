package bt7s7k7.vinf_project.spark;

import static org.apache.spark.sql.functions.array_join;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.regexp_extract_all;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import bt7s7k7.vinf_project.common.Logger;
import bt7s7k7.vinf_project.indexing.DocumentDatabase;

public final class SparkTest {
	public final DocumentDatabase documentDatabase;
	public final SparkSession spark;
	public final String inputPath;

	public SparkTest(DocumentDatabase documentDatabase, String inputPath) {
		this.documentDatabase = documentDatabase;
		this.inputPath = inputPath;

		Logger.info("Starting spark session...");

		this.spark = SparkSession.builder()
				.config("spark.master", "local")
				.config("spark.ui.showConsoleProgress", true)
				.getOrCreate();

		Logger.success("Started, " + this.spark.version());

	}

	public Dataset<Row> getInputData() {
		var start = Instant.now();

		Logger.info("Reading...");
		Dataset<Row> df;

		if (this.inputPath.endsWith(".parquet")) {
			df = this.spark.read()
					.parquet(this.inputPath);
		} else {
			// Read XML by default
			df = this.spark.read()
					.option("rowTag", "page")
					.option("inferSchema", false)
					.schema(WikipediaSchema.FULL_SCHEMA)
					.xml(this.inputPath);
		}

		var end = Instant.now();
		Logger.success("Done. Took: " + Duration.between(start, end));

		return df;
	}

	public void convert() {
		var start = Instant.now();
		var df = this.getInputData();

		Logger.info("Converting...");

		df.write()
				.option("compression", "gzip")
				.parquet(this.inputPath + ".parquet");

		var end = Instant.now();
		Logger.success("Done. Took: " + Duration.between(start, end));
	}

	public void run() {
		this.spark.udf().register("findAttributes", (UDF1<String, String>) text -> {
			var resultBuilder = new StringBuilder();

			try {
				AttributeExtractor.VALUE.findAttributes(text, attribute -> {
					if (resultBuilder.length() > 0) {
						resultBuilder.append("\t");
					}

					resultBuilder.append(attribute);
				});
			} catch (StackOverflowError error) {
				if (resultBuilder.length() > 0) {
					resultBuilder.append("\t");
				}

				resultBuilder.append("__stackOverflow");
			}

			return resultBuilder.toString();
		}, DataTypes.StringType);

		var df = this.getInputData();

		var start = Instant.now();

		Logger.info("Processing...");

		// Find rows with relevant titles
		var relevant = df
				.where(col("ns").equalTo(0))
				.select(
						col("title"),
						col("revision.text._VALUE").alias("value"))
				.withColumn("categories", lower(array_join(regexp_extract_all(col("value"), lit("\\[\\[Category:(.*?)[\\]|]"), lit(1)), "|")))
				.filter(
						col("categories").rlike("computers|(?<!word )processor|game consoles")
								.and(col("categories").rlike(
										"""
												fashion|people|theoretical computer science|companies|algorithm|programming constructs\
												|architecture statements|book|video game(?! consoles)|jargon|comic|culture\
												|lists? |operating system|system administration|characters in""")
										.unary_$bang()))
				.withColumn("attributes", callUDF("findAttributes", col("value")))
				.select(col("title"), col("attributes"))
				.orderBy(col("title"));

		try {
			Files.write(Path.of(this.inputPath + ".relevant.tsv"), (Iterable<String>) relevant.collectAsList().stream()
					.map(row -> (row.getString(0) + "\t" + row.getString(1)))::iterator);
			Files.write(Path.of(this.inputPath + ".relevant.txt"), (Iterable<String>) relevant.collectAsList().stream()
					.map(row -> row.getString(0))::iterator);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		var end = Instant.now();
		Logger.success("Done. Took: " + Duration.between(start, end));
	}
}
