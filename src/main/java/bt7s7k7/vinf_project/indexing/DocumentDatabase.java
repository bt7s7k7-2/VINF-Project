package bt7s7k7.vinf_project.indexing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.HashBiMap;

public class DocumentDatabase {
	public final Path path;

	public DocumentDatabase(Path path) {
		this.path = path;
	}

	protected HashBiMap<Integer, String> documentNameMapping = HashBiMap.create();

	public boolean hasDocument(String name) {
		return this.documentNameMapping.containsValue(name);
	}

	public int addDocument(String name) {
		// Get unique id for document
		var id = this.documentNameMapping.size();
		while (this.documentNameMapping.containsKey(id)) {
			id++;
		}
		this.documentNameMapping.put(id, name);
		return id;
	}

	public String findDocumentByIndex(int index) {
		return this.documentNameMapping.get(index);
	}

	public void reload() throws IOException {
		this.documentNameMapping.clear();

		// If the database file does not exist, start with empty database
		if (!Files.exists(this.path)) return;

		// Load all documents from TSV
		for (var line : Files.readAllLines(this.path)) {
			// Ignore empty lines
			if (StringUtils.isBlank(line)) continue;

			var segments = line.split("\t", 2);
			var id = Integer.parseInt(segments[0]);
			var name = segments[1];
			this.documentNameMapping.put(id, name);
		}
	}

	public void save() throws IOException {
		Files.write(this.path, (Iterable<String>) this.documentNameMapping.entrySet().stream()
				// Save documents in a TSV format, mapping id to name
				.map(kv -> kv.getKey() + "\t" + kv.getValue())::iterator);
	}
}
