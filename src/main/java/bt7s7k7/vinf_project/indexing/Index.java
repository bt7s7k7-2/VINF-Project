package bt7s7k7.vinf_project.indexing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

public class Index {
	public final Path path;

	public Index(Path path) {
		this.path = path;
	}

	protected TreeMap<String, TermInfo> terms = new TreeMap<>();

	public TermInfo getTerm(String name) {
		var existing = this.terms.get(name);
		if (existing != null) return existing;

		var term = new TermInfo(name, 0);
		this.terms.put(name, term);
		return term;
	}

	public void reload() throws IOException {
		this.terms.clear();

		// If the database file does not exist, start with empty database
		if (!Files.exists(this.path)) return;

		// Load all documents from TSV
		for (var line : Files.readAllLines(this.path)) {
			// Ignore empty lines
			if (StringUtils.isBlank(line)) continue;

			var segments = line.split("\t");
			// First column is the term
			var name = segments[0];
			// Second column is the total frequency
			var totalFrequency = Integer.parseInt(segments[1]);

			var term = new TermInfo(name, totalFrequency);

			// All documents that contain this term are specified by a pair of columns, the document ID an frequency
			for (int i = 2; i < segments.length; i += 2) {
				var document = Integer.parseInt(segments[i]);
				var frequency = Integer.parseInt(segments[i + 1]);

				term.locations.add(new TermInfo.Location(document, frequency));
			}

			this.terms.put(term.value, term);
		}
	}

	public void save() throws IOException {
		Files.write(this.path, (Iterable<String>) this.terms.values().stream()
				// Save documents in a TSV format
				.map(term -> Stream.concat(
						// First two columns are the term and the total frequency
						Stream.of(term.value, "" + term.totalFrequency),
						// For each location, add column for document ID and frequency
						term.locations.stream().flatMap(location -> Stream.of(
								"" + location.document(),
								"" + location.frequency())))
						.collect(Collectors.joining("\t")))::iterator);
	}
}
