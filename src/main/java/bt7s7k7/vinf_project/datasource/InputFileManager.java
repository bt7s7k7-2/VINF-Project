package bt7s7k7.vinf_project.datasource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;

public class InputFileManager {
	public final Path path;
	protected HashSet<InputFile> cachedFiles = new HashSet<>();

	public InputFileManager(Path path) {
		this.path = path;
	}

	public void refreshCache() throws IOException {
		this.cachedFiles.clear();
		try (var reader = Files.newDirectoryStream(this.path)) {
			for (var path : reader) {
				if (!Files.isRegularFile(path)) continue;
				if (!path.toString().endsWith(".html")) continue;

				var inputFile = InputFile.fromCachedFile(path);
				this.cachedFiles.add(inputFile);
			}
		}
	}

	public void addFile(InputFile file) throws IOException {
		var added = this.cachedFiles.add(file);
		if (!added) {
			throw new RuntimeException("Duplicate addition of input file " + file.name);
		}

		this.cachedFiles.add(file);
		file.save(this.path);
	}

	public boolean hasFileWithName(String name) {
		return this.cachedFiles.contains(InputFile.fromName(name));
	}
}
