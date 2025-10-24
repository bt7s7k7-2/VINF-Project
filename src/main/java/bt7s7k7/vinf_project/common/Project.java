package bt7s7k7.vinf_project.common;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import bt7s7k7.vinf_project.indexing.DocumentDatabase;
import bt7s7k7.vinf_project.indexing.Index;
import bt7s7k7.vinf_project.input.InputFileManager;

public class Project {
	public final Path rootPath;

	/**
	 * To ensure the crawler does not visit sites other than the target site, all URLs will be
	 * tested to start with this string.
	 */
	protected final URI prefix;
	protected final String prefixString;

	protected Project(Path rootPath, URI prefix) {
		this.rootPath = rootPath;
		this.prefix = prefix;
		this.prefixString = this.prefix.toString();
	}

	public URI getRelativeUri(URI absolute) {
		return this.prefix.relativize(absolute);
	}

	public URI getAbsoluteURI(String relative) {
		return this.prefix.resolve("./" + relative);
	}

	public boolean isUrlPartOfTargetSize(URI url) {
		return url.toString().startsWith(this.prefixString);
	}

	public InputFileManager getInputFileManager() throws IOException {
		var inputFilesPath = this.rootPath.resolve("input");
		// In a fresh project the input directory may not exist, so create it
		Files.createDirectories(inputFilesPath);

		var manager = new InputFileManager(inputFilesPath);
		manager.refreshCache();

		return manager;
	}

	public DocumentDatabase getDocumentDatabase() throws IOException {
		var path = this.rootPath.resolve("documents.tsv");
		var database = new DocumentDatabase(path);

		database.reload();

		return database;
	}

	public Index getIndex() throws IOException {
		var path = this.rootPath.resolve("index.tsv");
		var index = new Index(path);

		index.reload();

		return index;
	}

	/** Creates a project at a path. If the project folder does not exist it is created. */
	public static Project fromPath(Path rootPath, URI prefix) throws IOException {
		Files.createDirectories(rootPath);
		return new Project(rootPath, prefix);
	}
}
