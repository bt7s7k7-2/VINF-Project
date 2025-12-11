package bt7s7k7.vinf_project.spark;

import static org.apache.spark.sql.functions.array_join;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.regexp_extract_all;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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
import bt7s7k7.vinf_project.indexing.Indexer;
import bt7s7k7.vinf_project.indexing.TextExtractor;

public class ExtractionPipeline {
	public final Project project;

	public ExtractionPipeline(Project project) {
		this.project = project;
	}

	public static interface PipelineStage {
		public void execute(String target) throws Exception;
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

	public void execute(String target, PipelineStage stage) throws Exception {
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

	private static final Pattern SITE_CATEGORIES_PATTERN = Pattern.compile("\\[\\[Categor(?:ies|y)\\]\\]: ((?:\\[\\[.*?\\]\\])+)");
	private static final Pattern SITE_CATEGORY_PATTERN = Pattern.compile("\\[\\[(.*?)\\]\\]");
	private static final Pattern SITE_WHITELIST = Pattern.compile("system|processor|computer|mainframe|unibus", Pattern.CASE_INSENSITIVE);
	private static final Pattern SITE_BLACKLIST = Pattern.compile("operating system|file system|software|manufacturer|documentation|families|interface| basics", Pattern.CASE_INSENSITIVE);
	private static final Pattern TITLE_ILLEGAL = Pattern.compile("\\(.*?\\)", Pattern.CASE_INSENSITIVE);

	public Path getSiteAttributes() throws IOException {
		var output = this.getIntermediateFolder().resolve("attr_site.parquet");

		if (Files.exists(output)) {
			Logger.warn("Site attributes file already exists");
			return output;
		}

		var start = new Stopwatch("Reading file contents");
		var inputFiles = this.project.getInputFileManager();
		var rows = new ArrayList<Row>();

		for (var file : inputFiles.getFiles()) {
			var content = Indexer.getDocumentContentIfValid(file, inputFiles);
			if (content == null) continue;

			var text = TextExtractor.extractText(content);
			var categoriesMatcher = SITE_CATEGORIES_PATTERN.matcher(text);
			if (!categoriesMatcher.find()) continue;
			var categoryMatcher = SITE_CATEGORY_PATTERN.matcher(categoriesMatcher.group(1));
			var categories = new StringBuilder();

			while (categoryMatcher.find()) {
				if (!categories.isEmpty()) categories.append("|");
				categories.append(categoryMatcher.group(1));
			}

			var categoriesString = categories.toString();
			var relevant = SITE_WHITELIST.matcher(categoriesString).find() && !SITE_BLACKLIST.matcher(categoriesString).find();
			if (!relevant) continue;

			var title = file.name.replace("_", " ");
			rows.add(RowFactory.create(title, text));
		}

		start.close();

		var spark = this.getSpark();

		start = new Stopwatch("Creating data frame");

		var df = spark.createDataFrame(rows, new StructType(new StructField[] {
				new StructField("title", DataTypes.StringType, false, Metadata.empty()),
				new StructField("value", DataTypes.StringType, false, Metadata.empty()),
		}));

		var result = df
				.withColumn("attributes", callUDF("findAttributes", col("value")))
				.orderBy(col("title"));

		result.write().parquet(output.toString());

		start.close();

		return output;
	}

	public Dataset<Row> tryJoin(String target) throws IOException {
		var externInput = this.getAttributes(target);
		var siteInput = this.getSiteAttributes();
		var spark = this.getSpark();

		try (var __ = new Stopwatch("Joining attributes...")) {
			var dfExtern = spark.read().parquet(externInput.toString());
			var dfSite = spark.read().parquet(siteInput.toString());

			spark.udf().register("normalizeTitle", (UDF1<String, String>) title -> TextExtractor.extractTokens(TITLE_ILLEGAL.matcher(title).replaceAll("")).collect(Collectors.joining(" ")), DataTypes.StringType);

			return dfExtern
					.withColumn("nTitle", callUDF("normalizeTitle", col("title")))
					.alias("extern")
					.join(
							dfSite
									.withColumn("nTitle", callUDF("normalizeTitle", col("title")))
									.alias("site"),
							col("extern.nTitle").equalTo(col("site.nTitle")), "outer")
					.select(
							coalesce(col("extern.title"), col("site.title")).alias("title"),
							col("extern.title").alias("externTitle"),
							col("site.title").alias("siteTitle"),
							concat_ws("\t", col("site.attributes"), col("extern.attributes")).alias("attributes"),
							concat_ws(" ", col("site.value"), col("extern.value")).alias("value"));
		}
	}

	public static double findMostProbableValue(Stream<Double> input) {
		// To find a most probable value from a set of values, get the one that occurs the most. If
		// there is a tie, be pessimistic and pick the smallest one
		return input
				.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
				.entrySet().stream()
				// First order by frequency
				.max(Comparator.<Map.Entry<Double, Long>>comparingLong(Map.Entry::getValue)
						// Then order from smallest to largest
						.thenComparing(Map.Entry::getKey, Comparator.reverseOrder()))
				.orElseGet(() -> {
					throw new RuntimeException("Failed to find value");
				}).getKey().intValue();
	}

	private static double parseNumber(String number) {
		try {
			return switch (number.toLowerCase()) {
				case "sixteen" -> 16;
				case "single" -> 1;
				case "four" -> 4;
				default -> Double.parseDouble(number);
			};
		} catch (NumberFormatException error) {
			Logger.error("Failed to parse number: " + number);
			return Double.POSITIVE_INFINITY;
		}
	}

	public void createLuceneIndex(String target) throws IOException {
		var output = this.getOutputFolder().resolve("index");
		var df = this.tryJoin(target);

		try (var __ = new Stopwatch("Creating index")) {
			try (var directory = FSDirectory.open(output)) {
				var analyzer = new StandardAnalyzer();
				var config = new IndexWriterConfig(analyzer);
				config.setOpenMode(OpenMode.CREATE);

				try (var writer = new IndexWriter(directory, config)) {

					for (var row : df.collectAsList()) {
						var document = new Document();

						var title = row.getString(row.fieldIndex("title"));
						document.add(new Field("title", title, TextField.TYPE_STORED));

						var siteTitle = row.getString(row.fieldIndex("siteTitle"));
						if (siteTitle != null) document.add(new StoredField("siteTitle", siteTitle));

						var externTitle = row.getString(row.fieldIndex("externTitle"));
						if (externTitle != null) document.add(new StoredField("externTitle", externTitle));

						var text = row.getString(row.fieldIndex("value"));
						document.add(new Field("text", text, TextField.TYPE_NOT_STORED));

						// Parse out the attributes from string
						var attributesString = row.getString(row.fieldIndex("attributes"));
						var attributes = new HashMap<String, List<String>>();
						for (var kvString : attributesString.split("\t")) {
							if (kvString.isBlank()) continue;
							var delimiter = kvString.indexOf(":");
							if (delimiter == -1) {
								Logger.warn("Skipping attribute " + kvString + ", in document " + title);
								continue;
							}

							var key = kvString.substring(0, delimiter);
							if (key.isBlank()) {
								if (key.length() == 0) {
									throw new RuntimeException("Failed to parse key from " + kvString + ", in document " + title);
								} else {
									Logger.warn("Key is blank for document " + title);
									continue;
								}
							}

							var value = kvString.substring(delimiter + 1, kvString.length());
							if (value.isBlank()) {
								if (value.length() == 0) {
									throw new RuntimeException("Failed to parse value from " + kvString + ", in document " + title);
								} else {
									Logger.warn("Value is blank for document " + title);
									continue;
								}
							}

							attributes.computeIfAbsent(key, ___ -> new ArrayList<>()).add(value);
						}

						// Index companies, prioritise companies from infobox, else use the companies from text
						for (var company : attributes.getOrDefault("company", attributes.getOrDefault("company?", Collections.emptyList()))) {
							document.add(new Field("company", company, TextField.TYPE_STORED));
						}

						// Index release date
						do {
							// Prioritise values from infobox, else use the values from text
							var values = attributes.getOrDefault("release", attributes.getOrDefault("release?", Collections.emptyList()));
							if (values.isEmpty()) break;

							var value = (int) findMostProbableValue(values.stream().map(ExtractionPipeline::parseNumber));

							document.add(new IntPoint("release", value));
							// Because IntPoint does not store the value, store it here
							document.add(new StoredField("release", value));
						} while (false);

						// Index word size
						do {
							// Prioritise values from infobox, else use the values from text
							var values = attributes.getOrDefault("wordSize", attributes.getOrDefault("wordSize?", Collections.emptyList()));
							if (values.isEmpty()) break;

							var value = (int) findMostProbableValue(values.stream().map(ExtractionPipeline::parseNumber));

							document.add(new IntPoint("wordSize", value));
							// Because IntPoint does not store the value, store it here
							document.add(new StoredField("wordSize", value));
						} while (false);

						// Index clock speed
						do {
							// Prioritise values from infobox, else use the values from text
							var values = attributes.getOrDefault("clockSpeed", attributes.getOrDefault("clockSpeed?", Collections.emptyList()));
							if (values.isEmpty()) break;

							var value = findMostProbableValue(values.stream().map(speed -> {
								var segments = speed.split(" ");
								var number = parseNumber(segments[0]);
								var unit = segments[1];

								return switch (unit.toLowerCase()) {
									case "hz" -> number;
									case "khz" -> number * 1000;
									case "mhz" -> number * 1000 * 1000;
									case "ghz" -> number * 1000 * 1000 * 1000;
									default -> throw new IllegalStateException("Invalid unit " + unit);
								};
							}));

							document.add(new DoublePoint("clockSpeed", value));
							// Because IntPoint does not store the value, store it here
							document.add(new StoredField("clockSpeed", value));
						} while (false);

						// Index technology
						{
							var tech = attributes.get("tech");
							if (tech == null) tech = Collections.emptyList();

							if (tech.contains("serial")) {
								document.add(new Field("architecture", "serial", TextField.TYPE_STORED));
							} else {
								document.add(new Field("architecture", "parallel", TextField.TYPE_STORED));
							}

							if (tech.contains("vacuum-tubes")) {
								document.add(new Field("logic", "vacuum-tubes", TextField.TYPE_STORED));
							} else if (tech.contains("electro-mechanical")) {
								document.add(new Field("logic", "electro-mechanical", TextField.TYPE_STORED));
							} else {
								document.add(new Field("logic", "transistor", TextField.TYPE_STORED));
							}
						}

						writer.addDocument(document);
					}
				}
			}
		}
	}

	private static final void addInfo(StringBuilder builder, String info, String value) {
		if (!builder.isEmpty()) builder.append("; ");
		builder.append(info);
		builder.append(": ");
		builder.append(value);
	}

	private static final void addUrlInfo(StringBuilder builder, Document document, String key, String base, String label) throws URISyntaxException {
		var value = document.get(key);
		if (value == null) return;
		var url = URI.create(base).resolve(new URI(null, null, value, null, null));
		addInfo(builder, label, url.toString());
	}

	public void searchLuceneIndex() throws IOException, URISyntaxException {
		var output = this.getOutputFolder().resolve("index");

		try (var directory = FSDirectory.open(output)) {
			try (DirectoryReader reader = DirectoryReader.open(directory)) {
				var searcher = new IndexSearcher(reader);
				var queryParser = new StandardQueryParser(new StandardAnalyzer());

				queryParser.setPointsConfigMap(Map.of(
						"wordSize", new PointsConfig(DecimalFormat.getNumberInstance(), Integer.class),
						"release", new PointsConfig(DecimalFormat.getNumberInstance(), Integer.class),
						"clockSpeed", new PointsConfig(DecimalFormat.getNumberInstance(), Double.class)));

				queryParser.setMultiFields(new CharSequence[] {
						"title", "text", "company", "architecture", "logic"
				});

				queryParser.setFieldsBoost(Map.of(
						"text", 1.0f,
						"title", 4.0f,
						"company", 5.0f,
						"architecture", 5.0f,
						"logic", 5.0f));

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

						var query = queryParser.parse(line, null);
						var hits = searcher.search(query, 100).scoreDocs;
						var storedFields = searcher.storedFields();

						for (var hit : hits) {
							var document = storedFields.document(hit.doc);
							var title = document.get("title");
							var infoBuilder = new StringBuilder();
							addInfo(infoBuilder, "Score", "" + hit.score);
							addUrlInfo(infoBuilder, document, "externTitle", "https://en.wikipedia.org/wiki/", "Wikipedia");
							addUrlInfo(infoBuilder, document, "siteTitle", "https://gunkies.org/wiki/", "Site");
							Logger.info("Found: " + title + " \u001b[2m(" + infoBuilder.toString() + ")\u001b[0m");
						}

						Logger.success("Found " + hits.length + " results");

					} catch (UserInterruptException __) {
						continue;
					} catch (EndOfFileException __) {
						break;
					} catch (QueryNodeException error) {
						Logger.error("Failed to parse query: " + error.getLocalizedMessage());
					}
				}

				terminal.close();
			}
		}
	}
}
