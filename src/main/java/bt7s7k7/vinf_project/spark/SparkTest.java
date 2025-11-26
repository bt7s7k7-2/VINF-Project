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
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.TerminalBuilder;

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

	public List<Row> run() throws IOException {
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
				.select(col("title"), col("attributes"), col("value"))
				.orderBy(col("title"));

		var results = relevant.collectAsList();

		Files.write(Path.of(this.inputPath + ".relevant.tsv"), (Iterable<String>) results.stream()
				.map(row -> (row.getString(0) + "\t" + row.getString(1)))::iterator);
		Files.write(Path.of(this.inputPath + ".relevant.txt"), (Iterable<String>) results.stream()
				.map(row -> row.getString(0))::iterator);

		var end = Instant.now();
		Logger.success("Done. Took: " + Duration.between(start, end));

		return results;
	}

	public void createLuceneIndex() throws IOException {
		var rows = this.run();
		var indexPath = Paths.get("./project/lucene");

		Logger.info("Creating index...");
		var start = Instant.now();

		try (var directory = FSDirectory.open(indexPath)) {
			var analyzer = new StandardAnalyzer();
			var config = new IndexWriterConfig(analyzer);
			config.setOpenMode(OpenMode.CREATE);
			try (var writer = new IndexWriter(directory, config)) {
				for (var row : rows) {
					var document = new Document();

					var title = row.getString(row.fieldIndex("title"));
					document.add(new Field("title", title, TextField.TYPE_STORED));

					var text = row.getString(row.fieldIndex("value"));
					document.add(new Field("text", text, TextField.TYPE_NOT_STORED));

					writer.addDocument(document);
				}
			}
		}

		var end = Instant.now();
		Logger.success("Done. Took: " + Duration.between(start, end));
	}

	public static void searchLuceneIndex() throws IOException {
		var indexPath = Paths.get("./project/lucene");
		try (var directory = FSDirectory.open(indexPath)) {
			try (DirectoryReader reader = DirectoryReader.open(directory)) {
				var searcher = new IndexSearcher(reader);
				var parser = new QueryParser("text", new StandardAnalyzer());

				var terminal = TerminalBuilder.builder()
						.system(true)
						.build();

				var history = new DefaultHistory();

				var lineReader = LineReaderBuilder.builder()
						.terminal(terminal)
						.history(history)
						.build();

				while (true) {
					try {
						var line = lineReader.readLine("> ").trim();
						if (line.isEmpty()) continue;

						var query = parser.parse(line);
						var hits = searcher.search(query, 10).scoreDocs;
						var storedFields = searcher.storedFields();

						for (var hit : hits) {
							var document = storedFields.document(hit.doc);
							Logger.info("Found: " + document.get("title") + " \u001b[2m(Score: " + hit.score + ")\u001b[0m");
						}

						Logger.success("Found " + hits.length + " results");

					} catch (UserInterruptException __) {
						continue;
					} catch (EndOfFileException __) {
						break;
					} catch (ParseException error) {
						Logger.error("Failed to parse query: " + error.getLocalizedMessage());
					}
				}

				terminal.close();
			}
		}
	}
}
