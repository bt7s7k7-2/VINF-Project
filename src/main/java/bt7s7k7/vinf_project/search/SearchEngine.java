package bt7s7k7.vinf_project.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import bt7s7k7.vinf_project.common.Project;
import bt7s7k7.vinf_project.common.Support;
import bt7s7k7.vinf_project.indexing.DocumentDatabase;
import bt7s7k7.vinf_project.indexing.Index;
import bt7s7k7.vinf_project.indexing.TermInfo;
import bt7s7k7.vinf_project.indexing.TextExtractor;

public class SearchEngine {
	public final Project project;
	public final DocumentDatabase documentDatabase;
	public final Index index;

	public SearchEngine(Project project) throws IOException {
		this.project = project;
		this.documentDatabase = project.getDocumentDatabase();
		this.index = project.getIndex();
	}

	private static final Pattern COMMAND_PATTERN = Pattern.compile("-([a-z]+)");

	public static class Suggestion {
		public final int document;
		public double score;

		public Suggestion(int document) {
			this.document = document;
		}
	}

	public List<Suggestion> search(String query) {
		// Extract commands from input
		var commands = new HashSet<String>();
		query = COMMAND_PATTERN.matcher(query).replaceAll(match -> {
			commands.add(match.group(1));
			return "";
		});

		// Parse the query
		var tokens = TextExtractor.extractTokens(query);
		var terms = this.parseQuery(tokens);
		// Test if the query is valid
		if (terms == null) {
			return Collections.emptyList();
		}

		// Find matching documents and add them to the suggestion set
		var suggestions = this.getMatchingDocuments(terms)
				.mapToObj(Suggestion::new)
				.collect(Collectors.toCollection(ArrayList::new));

		var N = this.documentDatabase.getDocumentCount();
		// Calculate weights for each term in the query
		var queryWeights = terms.stream()
				.mapToDouble(term -> {
					var df = term.getDF();
					var idf = Math.log((double) N / df);
					return idf;
				})
				.toArray();

		for (var suggestion : suggestions) {
			// Calculate the weights for each term in the document that is relevant to the query
			var documentWeights = terms.stream()
					.mapToDouble(term -> {
						var tf = term.getTF(suggestion.document);
						var wf = tf > 0 ? 1 + Math.log(tf) : 0;
						return wf;
					})
					.toArray();

			// Perform euclidean normalization
			var normalizationFactor = 1 / Arrays.stream(documentWeights)
					.map(v -> v * v)
					.sum();

			// Calculate score
			double score = 0;
			for (int i = 0; i < documentWeights.length; i++) {
				var normalized = documentWeights[i] * normalizationFactor;
				score += normalized * queryWeights[i];
			}

			suggestion.score = score;
		}

		// Sort suggestions by score descending
		suggestions.sort(Comparator.comparing(v -> -v.score));
		return suggestions;
	}

	public List<TermInfo> parseQuery(Stream<String> tokens) {
		var terms = tokens
				// Remove duplicate tokens
				.distinct()
				// Get term from index for each token
				.map(this.index::findTerm)
				.toList();

		// Query is empty so it would match all documents
		if (terms.isEmpty()) {
			return null;
		}

		// Query contains a token which does not have a term in the index
		// so it won't find any documents
		if (terms.contains(null)) {
			return null;
		}

		return terms;
	}

	public IntStream getMatchingDocuments(List<TermInfo> terms) {
		List<TermInfo.Location> a = terms.get(0).locations;
		for (var i = 1; i < terms.size(); i++) {
			var b = terms.get(i).locations;

			a = Support.getIntersection(a, b, Comparator.comparing(TermInfo.Location::document));
		}

		return a.stream()
				// Get document ID
				.mapToInt(TermInfo.Location::document);
	}

}
