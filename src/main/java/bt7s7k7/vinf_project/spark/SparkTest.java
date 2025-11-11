package bt7s7k7.vinf_project.spark;

import java.time.Duration;
import java.time.Instant;

import org.apache.spark.sql.SparkSession;

import bt7s7k7.vinf_project.common.Logger;

public final class SparkTest {
	private SparkTest() {}

	public static void run() {
		Logger.info("Starting spark session...");

		var spark = SparkSession.builder()
				.config("spark.master", "local")
				.config("spark.ui.showConsoleProgress", true)
				.getOrCreate();

		Logger.success("Started, " + spark.version());

		var start = Instant.now();

		Logger.info("Reading...");

		var df = spark.read()
				.option("rowTag", "page")
				.option("inferSchema", false)
				.schema(WikipediaSchema.FULL_SCHEMA)
				.xml("xxx");

		System.out.println(df.count());

		var end = Instant.now();
		Logger.success("Done. Took: " + Duration.between(start, end));

		spark.stop();
	}
}
