package bt7s7k7.vinf_project.search;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
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

	public List<String> search(String query) {
		var tokens = TextExtractor.extractTokens(query);
		return this.getMatchingDocuments(tokens);
	}

	public List<String> getMatchingDocuments(Stream<String> tokens) {
		var terms = tokens
				// Remove duplicate tokens
				.distinct()
				// Get term from index for each token
				.map(this.index::findTerm).filter(Objects::nonNull)
				// Sort terms by amount of documents ascending
				.sorted(Comparator.comparingInt(TermInfo::getDocumentCount))
				.toList();

		// If all tokens in the query weren't in the index, return an empty result
		if (terms.isEmpty()) return Collections.emptyList();

		List<TermInfo.Location> a = terms.get(0).locations;
		for (var i = 1; i < terms.size(); i++) {
			var b = terms.get(i).locations;

			a = Support.getIntersection(a, b, Comparator.comparing(TermInfo.Location::document));
		}

		return a.stream()
				// Get document ID
				.mapToInt(TermInfo.Location::document)
				// Find document by ID
				.mapToObj(this.documentDatabase::findDocumentByIndex)
				.toList();
	}

}
