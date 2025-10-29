package bt7s7k7.vinf_project.indexing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

public class Index {
	public final Path path;

	public Index(Path path) {
		this.path = path;
	}

	public static record Location(int document, int frequency) {
		public static void put(List<Location> list, int document, int frequency) {
			var index = Collections.binarySearch(Lists.transform(list, Location::document), document);
			// Test if not found
			if (index >= 0) {
				list.set(index, new Location(document, frequency));
			} else {
				list.add(-index - 1, new Location(document, frequency));
			}
		}

		public static int get(List<Location> list, int document) {
			var index = Collections.binarySearch(Lists.transform(list, Location::document), document);
			// Test if not found
			if (index < 0) return 0;
			return list.get(index).frequency;
		}
	}

	protected Map<String, List<Location>> documentsByTerms = new TreeMap<>();
	protected Table<Integer, String, Integer> termsInDocuments = HashBasedTable.create();

	public int getDocumentCount() {
		return Index.this.termsInDocuments.rowKeySet().size();
	}

	public double getEuclideanNormalizer(int document) {
		// Get the inverse of the square root of the sum of the squares of each the frequency of each term in the document
		return 1 / Math.sqrt(this.termsInDocuments
				// Get term frequencies
				.row(document).values().stream()
				// Square
				.mapToDouble(v -> v.doubleValue() * v.doubleValue())
				// Sum
				.sum());
	}

	public int getTFMax(int document) {
		// Get all terms in the document
		return this.termsInDocuments.row(document)
				// Get frequencies
				.values().stream().mapToInt(Integer::valueOf)
				// Get maximum
				.max().orElse(0);
	}

	public class Term {
		public final String name;

		public Term(String name) {
			this.name = name;
		}

		public List<Location> getLocations() {
			return Index.this.documentsByTerms.computeIfAbsent(this.name, __ -> new ArrayList<>());
		}

		public void setFrequency(int document, int frequency) {
			Location.put(this.getLocations(), document, frequency);
			Index.this.termsInDocuments.put(document, this.name, frequency);
		}

		public int getDF() {
			return this.getLocations().size();
		}

		public int getTF(int document) {
			return Location.get(this.getLocations(), document);
		}

		public double getIDF() {
			return Math.log((double) Index.this.getDocumentCount() / this.getDF());
		}

		public List<Integer> getDocuments() {
			return this.getLocations().stream().map(Location::document).toList();
		}
	}

	public Term getTerm(String name) {
		return new Term(name);
	}

	public Stream<Term> getTerms() {
		return this.documentsByTerms.keySet().stream().map(this::getTerm);
	}

	public void reload() throws IOException {
		this.documentsByTerms.clear();
		this.termsInDocuments.clear();

		// If the database file does not exist, start with empty database
		if (!Files.exists(this.path)) return;

		// Load all documents from TSV
		for (var line : Files.readAllLines(this.path)) {
			// Ignore empty lines
			if (StringUtils.isBlank(line)) continue;

			var segments = line.split("\t");
			// First column is the term
			var name = segments[0];

			// All documents that contain this term are specified by a pair of columns, the document ID an frequency
			var termFrequency = new ArrayList<Location>();
			this.documentsByTerms.put(name, termFrequency);
			for (int i = 1; i < segments.length; i += 2) {
				var document = Integer.parseInt(segments[i]);
				var frequency = Integer.parseInt(segments[i + 1]);

				Location.put(termFrequency, document, frequency);
				this.termsInDocuments.put(document, name, frequency);
			}
		}
	}

	public void save() throws IOException {
		Files.write(this.path, (Iterable<String>) this.getTerms()
				// Save documents in a TSV format
				.map(term -> Stream.concat(
						// First two columns are the term and the total frequency
						Stream.of(term.name),
						// For each location, add column for document ID and frequency
						term.getLocations().stream().flatMap(location -> Stream.of(
								Integer.toString(location.document()),
								Integer.toString(location.frequency()))))
						.collect(Collectors.joining("\t")))::iterator);
	}
}
