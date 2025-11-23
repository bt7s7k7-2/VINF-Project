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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

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

	public static final class AttributeQueryBuilder {
		private static abstract class Builder<T> {
			private final T owner;

			public Builder(T owner) {
				this.owner = owner;
			}

			public T build() {
				return this.owner;
			}
		}

		private final List<Source<?>> sources = new ArrayList<>();

		public PatternSource<AttributeQueryBuilder> inPattern(String segment) {
			var source = new PatternSource<>(this, Pattern.compile(segment));
			this.sources.add(source);
			return source;
		}

		public Source<AttributeQueryBuilder> anywhere() {
			var source = new Source<>(this);
			this.sources.add(source);
			return source;
		}

		public void match(String value, Consumer<String> append) {
			for (var source : this.sources) {
				source.match(value, append);
			}
		}

		public static class Source<T> extends Builder<T> {
			protected final List<Attribute<?>> attributes = new ArrayList<>();

			public Source(T owner) {
				super(owner);
			}

			public Attribute<Source<T>> attribute(String name, String predicate, String value) {
				var attribute = new Attribute<>(this, name, predicate == null ? null : Pattern.compile(predicate), Pattern.compile(value));
				this.attributes.add(attribute);
				return attribute;
			}

			public void match(String value, Consumer<String> append) {
				for (var attribute : this.attributes) {
					attribute.match(value, append);
				}
			}
		}

		public static final class PatternSource<T> extends Source<T> {
			private final Pattern segment;

			public PatternSource(T owner, Pattern segment) {
				super(owner);
				this.segment = segment;
			}

			@Override
			public void match(String value, Consumer<String> append) {
				var matcher = this.segment.matcher(value);

				while (matcher.find()) {
					var input = matcher.group();
					for (var attribute : this.attributes) {
						if (attribute.match(input, append)) break;
					}
				}
			}
		}

		public static final class Attribute<T> extends Builder<T> {
			private final String name;
			private final Pattern predicate;
			private final Pattern value;

			private boolean multiple = false;

			public Attribute(T owner, String name, Pattern predicate, Pattern value) {
				super(owner);

				this.name = name;
				this.predicate = predicate;
				this.value = value;
			}

			public Attribute<T> hasMultiple() {
				this.multiple = true;
				return this;
			}

			public boolean match(String input, Consumer<String> append) {
				// If we have a predicate, it must be in input
				if (this.predicate != null && !this.predicate.matcher(input).find()) return false;

				var value = this.value.matcher(input);
				if (this.multiple) {
					var found = false;

					// Because we allow multiple value, go through all matches
					while (value.find()) {
						// Go through all groups in the value pattern and pick the first one with a value
						String groupValue = null;
						for (int i = 1; i <= value.groupCount(); i++) {
							groupValue = value.group(i);
							if (groupValue != null) break;
						}

						if (groupValue != null) {
							append.accept(this.name + ":" + groupValue);
							found = true;
						}
					}

					return found;
				}

				// Because we don't allow multiple value, go through only the first match
				if (value.find()) {
					// Go through all groups in the value pattern and pick the first one with a value
					String groupValue = null;
					for (int i = 1; i <= value.groupCount(); i++) {
						groupValue = value.group(i);
						if (groupValue != null) break;
					}

					if (groupValue != null) {
						append.accept(this.name + ":" + groupValue);
						return true;
					}
				}

				return false;
			}
		}

	}

	private static final AttributeQueryBuilder ATTRIBUTES = new AttributeQueryBuilder();

	static {
		// Match an named entity in a infobox, usually the target of a link, handle both [[entity]] and [[entity|label]], but could also be a plain string which is matched by "= <value>"
		var entityPattern = "\\[\\[([^|\\]]*?)(?:\\|.*?)?\\]\\]|= ([\\w- ]+)";

		// Match fields in infoboxes, these are definitely accurate
		ATTRIBUTES
				.inPattern("(?<=^|\\n)\\| *[^\\n]*? *= *((?:\\{\\{(?:.*?\\n?)+\\}\\}|\\[\\[.*?\\]\\]|.*?(?=\\n|$|\\|))(?: *,?))+")
				.attribute("release", "(?:release\\w*(?: date)?|produced-start) *=", "(\\d{4})").build()
				.attribute("discontinued", "(?:discontinued|produced-end) *=", "(\\d{4})").build()
				.attribute("wordSize", "data-width *=", "(\\d+)").hasMultiple().build()
				.attribute("wordSize", "platform *=", "\\[\\[(\\d+)-bit").hasMultiple().build()
				.attribute("manufacturer", "manuf(?:1|acturer) *=", entityPattern).hasMultiple().build()
				.attribute("developer", "(?:developer|designfirm) *=", entityPattern).hasMultiple().build()
				.attribute("owner", "owner *=", entityPattern).hasMultiple().build()
				.attribute("soldby", "soldby *=", entityPattern).hasMultiple().build()
				.build();

		// Find attributes in plain text directly, these are potentially accurate
		ATTRIBUTES
				.anywhere()
				.attribute("wordSize?", null, "\\[\\[(\\d+)-bit").build()
				.attribute("wordSize?", null, "(\\w+)-bit (?:word|processor|microprocessor)").build()
				.build();
	}

	public void run() {
		this.spark.udf().register("findAttributes", (UDF1<String, String>) text -> {
			var resultBuilder = new StringBuilder();

			ATTRIBUTES.match(text, attribute -> {
				if (resultBuilder.length() > 0) {
					resultBuilder.append("\t");
				}

				resultBuilder.append(attribute);
			});

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
						col("categories").rlike("computer")
								.and(col("categories").rlike("people|theoretical computer science|companies|algorithm|programming constructs|architecture statements|book|video game(?! consoles)|jargon|comic|culture|lists? ").unary_$bang()))
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
