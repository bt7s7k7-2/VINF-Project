package bt7s7k7.vinf_project.datasource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

import bt7s7k7.vinf_project.common.Support;

public class InputFile {

	public final String name;

	protected boolean saved;
	protected String content;

	protected InputFile(String name, boolean saved, String content) {
		this.name = name;
		this.saved = saved;
		this.content = content;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) return true;
		if (obj instanceof InputFile other) return this.name.equals(other.name);
		return false;
	}

	@Override
	public int hashCode() {
		return this.name.hashCode();
	}

	public void save(Path directory) throws IOException {
		// Prevent duplicate saving of an already saved file. This can happen whe save() is called
		// on a file that was loaded from cache.
		if (this.saved) return;

		var name = nameToFilename(this.name);
		var path = directory.resolve(name);
		this.saved = true;
		Files.writeString(path, this.content);
	}

	public static InputFile fromCachedFile(Path cachedFile) {
		var name = filenameToName(cachedFile.getFileName().toString());
		return new InputFile(name, true, null);
	}

	public static InputFile fromName(String name) {
		return new InputFile(name, true, null);
	}

	public static InputFile fromContent(String name, String content) {
		return new InputFile(name, false, content);
	}

	private final static Pattern INVALID_FILENAME_CHARACTERS = Pattern.compile("[^\\w-.]");

	public static String nameToFilename(String name) {
		// Replace all character that are not valid for filenames with escape sequences
		return Support.replaceAll(name, INVALID_FILENAME_CHARACTERS,
				v -> "(" + Integer.toHexString(v.group(0).charAt(0)) + ")")
				// Append file extension
				+ ".html";
	}

	private final static Pattern ESCAPE_SEQUENCE = Pattern.compile("\\(([0-9a-f]+)\\)");

	public static String filenameToName(String name) {
		// Remote the file extension
		if (name.endsWith(".html")) {
			name = name.substring(0, name.length() - 5);
		}

		// Replace escape sequences with their characters
		return Support.replaceAll(name, ESCAPE_SEQUENCE, v -> "" + (char) Integer.parseInt(v.group(1), 16));
	}
}
