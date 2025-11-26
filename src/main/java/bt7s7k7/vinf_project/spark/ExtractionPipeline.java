package bt7s7k7.vinf_project.spark;

import static org.apache.spark.sql.functions.array_join;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.regexp_extract_all;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

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
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.TerminalBuilder;

import bt7s7k7.vinf_project.common.Logger;
import bt7s7k7.vinf_project.common.Project;

public class ExtractionPipeline {
	public final Project project;

	public ExtractionPipeline(Project project) {
		this.project = project;
	}

	public static interface PipelineStage {
		public void execute(String target) throws IOException;
	}

	private static String removeExtension(String name) {
		if (name.endsWith(".bz2")) {
			name = name.substring(0, name.length() - 4);
		}

		return name;
	}

	private boolean inputFolderCreated = false;

	public static class Stopwatch implements Closeable {
		protected final Instant start;

		public Stopwatch(String name) {
			Logger.info(name + "...");
			this.start = Instant.now();
		}

		@Override
		public void close() throws IOException {
			var end = Instant.now();
			Logger.success("Done. Took: " + Duration.between(this.start, end));
		}

	}

	protected Path getInputFolder() throws IOException {
		var path = this.project.rootPath.resolve("external/input");

		if (!this.inputFolderCreated) {
			Files.createDirectories(path);
		}

		return path;
	}

	private boolean intermediateFolderCreated = false;

	protected Path getIntermediateFolder() throws IOException {
		var path = this.project.rootPath.resolve("external/intermediate");

		if (!this.intermediateFolderCreated) {
			Files.createDirectories(path);
		}

		return path;
	}

	private boolean outputFolderCreated = false;

	protected Path getOutputFolder() throws IOException {
		var path = this.project.rootPath.resolve("external/output");

		if (!this.outputFolderCreated) {
			Files.createDirectories(path);
		}

		return path;
	}

	private SparkSession cachedSession = null;

	public SparkSession getSpark() {
		if (this.cachedSession != null) return this.cachedSession;

		Logger.info("Starting spark session...");

		this.cachedSession = SparkSession.builder()
				.config("spark.master", "local")
				.config("spark.ui.showConsoleProgress", true)
				.getOrCreate();

		this.cachedSession.udf().register("findAttributes", (UDF1<String, String>) text -> {
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

		Logger.success("Started, " + this.cachedSession.version());

		return this.cachedSession;
	}

	public void execute(String target, PipelineStage stage) throws IOException {
		var inputFolder = this.getInputFolder();

		if (target.equals("1") || target.equals("*")) {
			try (var reader = Files.newDirectoryStream(inputFolder)) {
				var onlyReadTheFirstFile = target.equals("1");
				for (var path : reader) {
					if (!Files.isRegularFile(path)) continue;
					if (!path.toString().endsWith(".bz2")) continue;
					stage.execute(removeExtension(path.getFileName().toString()));
					if (onlyReadTheFirstFile) break;
				}
			}
		} else {
			stage.execute(removeExtension(inputFolder.resolve(target).toString()));
		}
	}

	public Path getParquetFile(String target) throws IOException {
		var input = this.getInputFolder().resolve(target + ".bz2");
		var output = this.getIntermediateFolder().resolve(target + ".parquet");
		var spark = this.getSpark();

		if (Files.exists(output)) {
			Logger.warn("Parquet file already exists");
			return output;
		}

		try (var __ = new Stopwatch("Converting XML file")) {
			var df = spark.read()
					.option("rowTag", "page")
					.option("inferSchema", false)
					.schema(WikipediaSchema.FULL_SCHEMA)
					.xml(input.toString());

			df.write().parquet(output.toString());

			return output;
		}
	}

	private static final StructType ATTRIBUTES_SCHEMA = new StructType(new StructField[] {
			new StructField("title", DataTypes.StringType, false, Metadata.empty()),
			new StructField("attributes", DataTypes.StringType, false, Metadata.empty()),
			new StructField("value", DataTypes.StringType, false, Metadata.empty()),
	});

	public Path getAttributes(String target) throws IOException {
		var input = this.getParquetFile(target);
		var output = this.getIntermediateFolder().resolve("attr_" + target + ".parquet");
		var spark = this.getSpark();

		if (Files.exists(output)) {
			Logger.warn("Attributes file already exists");
			return output;
		}

		Dataset<Row> result;

		try (var __ = new Stopwatch("Extracting attributes")) {
			var df = spark.read()
					.schema(WikipediaSchema.FULL_SCHEMA)
					.parquet(input.toString());

			result = df
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

			result.write().parquet(output.toString());
		}

		return output;
	}

	public void createLuceneIndex(String target) throws IOException {
		var input = this.getAttributes(target);
		var output = this.getOutputFolder().resolve("index");
		var spark = this.getSpark();

		try (var __ = new Stopwatch("Creating index")) {
			try (var directory = FSDirectory.open(output)) {
				var analyzer = new StandardAnalyzer();
				var config = new IndexWriterConfig(analyzer);
				config.setOpenMode(OpenMode.CREATE);

				try (var writer = new IndexWriter(directory, config)) {
					var df = spark.read()
							.schema(ATTRIBUTES_SCHEMA)
							.parquet(input.toString());

					for (var row : df.collectAsList()) {
						var document = new Document();

						var title = row.getString(row.fieldIndex("title"));
						document.add(new Field("title", title, TextField.TYPE_STORED));

						var text = row.getString(row.fieldIndex("value"));
						document.add(new Field("text", text, TextField.TYPE_NOT_STORED));

						writer.addDocument(document);
					}
				}
			}
		}
	}

	public void searchLuceneIndex() throws IOException {
		var output = this.getOutputFolder().resolve("index");

		try (var directory = FSDirectory.open(output)) {
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
