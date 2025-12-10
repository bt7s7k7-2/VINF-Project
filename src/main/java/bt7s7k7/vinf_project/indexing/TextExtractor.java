package bt7s7k7.vinf_project.indexing;

import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class TextExtractor {
	private TextExtractor() {}

	// Matches all HTML tags with their attributes and including closing and self-closing tags
	private static final Pattern HTML_TAG_PATTERN = Pattern.compile("<[\\/!]?([\\w-]+)\\s*(?:\\s*[\\w-](?:=(?:\"[^\"]*\")|[^\\s\"]+)?\\s*)*\\/?>");
	private static final Pattern HTML_ESCAPE_SEQUENCE = Pattern.compile("&[a-z]+;");

	public static String extractText(String html) {
		var matcher = HTML_TAG_PATTERN.matcher(html);
		var prevIndex = 0;
		var result = new StringBuilder();

		var isInScript = false;

		while (matcher.find()) {
			// Only append text to output if it is not from script tags
			if (!isInScript) {
				result.append(html, prevIndex, matcher.start());
			}

			prevIndex = matcher.end();

			// Detect start and end of script tags, so they can be ignored
			var tagName = matcher.group(1);
			var match = matcher.group();
			var isClosing = match.startsWith("</");

			switch (tagName) {
				case "script" -> {
					isInScript = isClosing ? false : true;
				}
				case "a" -> {
					result.append(isClosing ? "]]" : "[[");
				}
			}
		}

		// Add the rest of the content to result
		result.append(html, prevIndex, html.length());

		// Replace HTML escape sequences with their character. Only these were used in the dataset
		return HTML_ESCAPE_SEQUENCE.matcher(result.toString()).replaceAll(v -> switch (v.group()) {
			case "&amp;" -> "&";
			case "&gt;" -> ">";
			case "&lt;" -> "<";
			case "&quot;" -> "\"";
			default -> throw new RuntimeException("Invalid escape sequence");
		});
	}

	// Matches all words and numbers. Also included are special characters '/' and '-' if they are inside of a word, for system names like: "VAX-11/780"
	private static final Pattern TOKEN_PATTERN = Pattern.compile("[a-zA-Z0-9](?:[a-zA-Z0-9-\\/]*[a-zA-Z0-9])?");
	// Special character delimiting tokens
	private static final Pattern TOKEN_DELIMITER_PATTERN = Pattern.compile("[-\\/]");

	public static final Stream<String> extractTokens(String text) {
		// Find all tokens
		return TOKEN_PATTERN.matcher(text.toLowerCase()).results()
				// Get token strings
				.flatMap(match -> {
					var token = match.group();

					// If special character are found
					if (TOKEN_DELIMITER_PATTERN.matcher(token).find()) {
						// Also include parts of tokens delimited by special characters, so for "combined-word", also emit "combined" and "word"
						var subTokens = TOKEN_DELIMITER_PATTERN.splitAsStream(token);
						return Stream.concat(Stream.of(token), subTokens);
					}

					return Stream.of(token);
				});
	}
}
