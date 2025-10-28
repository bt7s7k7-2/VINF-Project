package bt7s7k7.vinf_project.indexing;

import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Collectors;

import bt7s7k7.vinf_project.common.Logger;
import bt7s7k7.vinf_project.common.Project;
import bt7s7k7.vinf_project.input.InputFileManager;

public class Indexer {
	public final Project project;
	public final DocumentDatabase documentDatabase;
	public final Index index;
	public final InputFileManager inputFiles;

	public Indexer(Project project) throws IOException {
		this.project = project;

		this.documentDatabase = this.project.getDocumentDatabase();
		this.index = this.project.getIndex();
		this.inputFiles = this.project.getInputFileManager();
	}

	public void index() throws IOException {
		var index = -1;

		for (var file : this.inputFiles.getFiles()) {
			index++;
			// Skip category files
			if (file.name.startsWith("Category:")
					|| file.name.startsWith("Category_talk:")
					|| file.name.startsWith("Special:")) {
				continue;
			}

			// Skip already indexed files
			if (this.documentDatabase.hasDocument(file.name)) {
				Logger.warn("File " + file.name + " already indexed");
				continue;
			}

			var documentId = this.documentDatabase.addDocument(file.name);

			// Get tokens in document
			var content = this.inputFiles.getContent(file);
			var text = TextExtractor.extractText(content);
			var tokens = TextExtractor.extractTokens(text);

			// Count frequencies for tokens
			var frequencies = tokens.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
			for (var kv : frequencies.entrySet()) {
				var termName = kv.getKey();
				var frequency = kv.getValue();

				// Add this document to the index
				var term = this.index.getTerm(termName);
				term.setFrequency(documentId, frequency.intValue());
			}

			Logger.success("Indexed document " + file.name + " (" + index + "/" + this.inputFiles.size() + ")");
		}

		this.documentDatabase.save();
		this.index.save();
	}
}
