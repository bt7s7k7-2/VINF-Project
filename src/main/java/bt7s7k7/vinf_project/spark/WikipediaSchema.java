package bt7s7k7.vinf_project.spark;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/*
 Inferred schema by reading first chunk:

 root
 |-- id: long (nullable = true)
 |-- ns: long (nullable = true)
 |-- redirect: struct (nullable = true)
 |    |-- _title: string (nullable = true)
 |-- revision: struct (nullable = true)
 |    |-- comment: string (nullable = true)
 |    |-- contributor: struct (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- ip: string (nullable = true)
 |    |    |-- username: string (nullable = true)
 |    |-- format: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- minor: string (nullable = true)
 |    |-- model: string (nullable = true)
 |    |-- origin: long (nullable = true)
 |    |-- parentid: long (nullable = true)
 |    |-- sha1: string (nullable = true)
 |    |-- text: struct (nullable = true)
 |    |    |-- _VALUE: string (nullable = true)
 |    |    |-- _bytes: long (nullable = true)
 |    |    |-- _sha1: string (nullable = true)
 |    |    |-- _xml:space: string (nullable = true)
 |    |-- timestamp: timestamp (nullable = true)
 |-- title: string (nullable = true)
 */

public class WikipediaSchema {
	// Helper for creating simple StructField with nullable=true and empty metadata
	private static StructField field(String name, DataType dataType) {
		return new StructField(name, dataType, true, Metadata.empty());
	}

	// Define the schema
	public static final StructType FULL_SCHEMA = new StructType(new StructField[] {
			// Top-level fields
			field("id", DataTypes.LongType),
			field("ns", DataTypes.LongType),

			// redirect: struct (nullable = true)
			field("redirect", new StructType(new StructField[] {
					// |-- _title: string (nullable = true)
					field("_title", DataTypes.StringType)
			})),

			// revision: struct (nullable = true)
			field("revision", new StructType(new StructField[] {
					// |-- comment: string (nullable = true)
					field("comment", DataTypes.StringType),

					// |-- contributor: struct (nullable = true)
					field("contributor", new StructType(new StructField[] {
							// | |-- id: long (nullable = true)
							field("id", DataTypes.LongType),
							// | |-- ip: string (nullable = true)
							field("ip", DataTypes.StringType),
							// | |-- username: string (nullable = true)
							field("username", DataTypes.StringType)
					})),

					// | |-- format: string (nullable = true)
					field("format", DataTypes.StringType),
					// | |-- id: long (nullable = true)
					field("id", DataTypes.LongType),
					// | |-- minor: string (nullable = true)
					field("minor", DataTypes.StringType),
					// | |-- model: string (nullable = true)
					field("model", DataTypes.StringType),
					// | |-- origin: long (nullable = true)
					field("origin", DataTypes.LongType),
					// | |-- parentid: long (nullable = true)
					field("parentid", DataTypes.LongType),
					// | |-- sha1: string (nullable = true)
					field("sha1", DataTypes.StringType),

					// | |-- text: struct (nullable = true)
					field("text", new StructType(new StructField[] {
							// | | |-- _VALUE: string (nullable = true)
							field("_VALUE", DataTypes.StringType),
							// | | |-- _bytes: long (nullable = true)
							field("_bytes", DataTypes.LongType),
							// | | |-- _sha1: string (nullable = true)
							field("_sha1", DataTypes.StringType),
							// Note: The colon in "_xml:space" requires a specialized handling if used directly.
							// Depending on the version of spark-xml or how Spark handles the schema,
							// you might need to use a different field name or quotation. Sticking to the name as printed:
							// | | |-- _xml:space: string (nullable = true)
							field("_xml:space", DataTypes.StringType)
					})),

					// | |-- timestamp: timestamp (nullable = true)
					field("timestamp", DataTypes.TimestampType)
			})),

			// |-- title: string (nullable = true)
			field("title", DataTypes.StringType)
	});
}
