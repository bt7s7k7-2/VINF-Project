package bt7s7k7.vinf_project.input;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class InputFileManager {
	public final Path path;
	protected HashMap<String, InputFile> cachedFiles = new HashMap<>();

	public InputFileManager(Path path) {
		this.path = path;
	}

	public Iterable<InputFile> getFiles() {
		return this.cachedFiles.values()::iterator;
	}

	public InputFile findFile(String name) {
		return this.cachedFiles.get(name);
	}

	public int size() {
		return this.cachedFiles.size();
	}

	public String getContent(InputFile file) throws IOException {
		if (file.content != null) return file.content;
		file.load(this.path);
		return file.content;
	}

	public void refreshCache() throws IOException {
		this.cachedFiles.clear();
		try (var reader = Files.newDirectoryStream(this.path)) {
			for (var path : reader) {
				if (!Files.isRegularFile(path)) continue;
				if (!path.toString().endsWith(".html")) continue;

				var inputFile = InputFile.fromCachedFile(path);
				this.cachedFiles.put(inputFile.name, inputFile);
			}
		}
	}

	public void addFile(InputFile file) throws IOException {
		var added = this.cachedFiles.put(file.name, file) == null;
		if (!added) {
			throw new RuntimeException("Duplicate addition of input file " + file.name);
		}

		file.save(this.path);
	}

	public boolean hasFileWithName(String name) {
		return this.cachedFiles.containsKey(name);
	}
}
