package bt7s7k7.vinf_project.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.function.ToDoubleFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import bt7s7k7.vinf_project.common.Project;
import bt7s7k7.vinf_project.common.Support;
import bt7s7k7.vinf_project.indexing.DocumentDatabase;
import bt7s7k7.vinf_project.indexing.Index;
import bt7s7k7.vinf_project.indexing.Index.Term;
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

	private static final Pattern COMMAND_PATTERN = Pattern.compile("-([a-z-]+)");

	public static class Suggestion {
		public final int document;
		public double score;

		public Suggestion(int document) {
			this.document = document;
		}
	}

	public List<Suggestion> search(String queryString) {
		// Extract commands from input
		var commands = new HashSet<String>();
		queryString = COMMAND_PATTERN.matcher(queryString).replaceAll(match -> {
			commands.add(match.group(1));
			return "";
		});

		// Parse the query
		var tokens = TextExtractor.extractTokens(queryString);
		var query = this.parseQuery(tokens);
		// Test if the query is valid
		if (query == null) {
			return Collections.emptyList();
		}

		// Find matching documents and add them to the suggestion set
		var suggestions = this.getMatchingDocuments(query)
				.mapToObj(Suggestion::new)
				.collect(Collectors.toCollection(ArrayList::new));

		// Calculate document scores for sorting
		var useCosineSimilarity = commands.contains("cos");
		if (useCosineSimilarity) {
			this.calculateScoresCosineSimilarity(commands, query, suggestions);
		} else {
			this.calculateScoresBasic(commands, query, suggestions);
		}

		// Sort suggestions by score descending
		suggestions.sort(Comparator.comparing(v -> -v.score));
		return suggestions;
	}

	protected void calculateScoresBasic(HashSet<String> commands, List<Term> query, ArrayList<Suggestion> suggestions) {
		var N = this.index.getDocumentCount();

		// Calculate weights for each term in the query
		var queryWeights = query.stream()
				.mapToDouble(term -> {
					var df = term.getDF();
					var idf = Math.log((double) N / df);
					return idf;
				})
				.toArray();

		// Select modification
		var modifyTFLog = commands.contains("log");
		var modifyTFByMaxTF = commands.contains("max-tf");

		var useEuclideanNormalization = commands.contains("euc");

		for (var suggestion : suggestions) {
			ToDoubleFunction<Index.Term> getTF;

			if (modifyTFLog) {
				// Modify TF logarithmic
				getTF = term -> {
					var tf = term.getTF(suggestion.document);
					var wf = tf > 0 ? 1 + Math.log(tf) : 0;
					return wf;
				};
			} else if (modifyTFByMaxTF) {
				// Normalize TF by max tf
				var tfMax = this.index.getTFMax(suggestion.document);
				getTF = term -> {
					var tf = term.getTF(suggestion.document);
					return 0.5 + 0.5 * tf / tfMax;
				};
			} else {
				getTF = term -> term.getTF(suggestion.document);
			}

			// Calculate the weights for each term in the document that is relevant to the query
			var documentWeights = query.stream()
					.mapToDouble(getTF)
					.toArray();

			// Perform normalization, if enabled
			var normalizationFactor = useEuclideanNormalization ? this.index.getEuclideanNormalizer(suggestion.document) : 1;

			// Calculate score
			double score = 0;
			for (int i = 0; i < documentWeights.length; i++) {
				// Calculate normalized weight
				var normalized = documentWeights[i] * normalizationFactor;
				// Add the score
				score += normalized * queryWeights[i];
			}

			suggestion.score = score;
		}
	}

	protected void calculateScoresCosineSimilarity(HashSet<String> commands, List<Term> terms, ArrayList<Suggestion> suggestions) {
		// Make a set of terms in query for lookup
		var termsInQuery = terms.stream()
				.map(v -> v.name)
				.collect(Collectors.toSet());

		for (var suggestion : suggestions) {
			// Get terms in document
			var documentTerms = this.index.getTermsInDocument(suggestion.document);

			// Calculate the cosine similarity
			var documentSum = 0.0;
			var querySum = 0.0;
			var product = 0.0;
			for (var kv : documentTerms.entrySet()) {
				var termValue = kv.getKey();
				var documentFrequency = kv.getValue().doubleValue();
				var isInQuery = termsInQuery.contains(termValue);
				var queryFrequency = isInQuery ? 1 : 0;

				querySum += queryFrequency; // When x is 1 or 0, then x^2 == x
				documentSum += documentFrequency * documentFrequency;

				product += documentFrequency * queryFrequency;
			}

			var cosineSimilarity = product / (Math.sqrt(documentSum) * Math.sqrt(querySum));
			suggestion.score = cosineSimilarity;
		}
	}

	protected List<Index.Term> parseQuery(Stream<String> tokens) {
		var terms = tokens
				// Remove duplicate tokens
				.distinct()
				// Get term from index for each token
				.map(this.index::getTerm)
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

	protected IntStream getMatchingDocuments(List<Index.Term> terms) {
		var a = terms.get(0).getDocuments();
		for (var i = 1; i < terms.size(); i++) {
			var b = terms.get(i).getDocuments();

			a = Support.getIntersection(a, b, Comparator.naturalOrder());
		}

		return a.stream().mapToInt(Integer::valueOf);
	}
}
