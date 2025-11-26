package bt7s7k7.vinf_project.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class AttributeExtractor {
	private AttributeExtractor() {}

	private static abstract class Builder<T> {
		private final T owner;

		public Builder(T owner) {
			this.owner = owner;
		}

		public T build() {
			return this.owner;
		}
	}

	private final List<Source<?>> sources = new ArrayList<>();

	public PatternSource<AttributeExtractor> inPattern(String segment) {
		var source = new PatternSource<>(this, segment);
		this.sources.add(source);
		return source;
	}

	public Source<AttributeExtractor> anywhere() {
		var source = new Source<>(this);
		this.sources.add(source);
		return source;
	}

	public void findAttributes(String value, Consumer<String> append) {
		for (var source : this.sources) {
			source.match(value, append);
		}
	}

	public static class Source<T> extends Builder<T> {
		protected final List<Attribute<?>> attributes = new ArrayList<>();

		public Source(T owner) {
			super(owner);
		}

		public Attribute<Source<T>> attribute(String name, String predicate, String value) {
			var attribute = new Attribute<>(this, name, predicate, value);
			this.attributes.add(attribute);
			return attribute;
		}

		public void match(String value, Consumer<String> append) {
			for (var attribute : this.attributes) {
				attribute.match(value, append);
			}
		}
	}

	public static final class PatternSource<T> extends Source<T> {
		private final Pattern segment;
		private boolean multiple = false;

		public PatternSource(T owner, Pattern segment) {
			super(owner);
			this.segment = segment;
		}

		public PatternSource(T owner, String segment) {
			this(owner, Pattern.compile(segment));
		}

		public PatternSource<T> allowMultiple() {
			this.multiple = true;
			return this;
		}

		@Override
		public void match(String value, Consumer<String> append) {
			var matcher = this.segment.matcher(value);

			while (matcher.find()) {
				var input = matcher.group();
				for (var attribute : this.attributes) {
					if (attribute.match(input, append)) {
						if (!this.multiple) break;
					}
				}
			}
		}
	}

	public static final class Attribute<T> extends Builder<T> {
		private final String name;
		private final Pattern predicate;
		private final Pattern value;
		private List<Attribute<?>> children = null;

		private boolean multipleMatches = false;
		private boolean concatGroups = false;

		public Attribute(T owner, String name, Pattern predicate, Pattern value) {
			super(owner);

			this.name = name;
			this.predicate = predicate;
			this.value = value;

			if ((name == null) != (value == null)) throw new IllegalArgumentException("The name and value of an Attribute must both be null or not-null");
		}

		public Attribute(T owner, String name, String predicate, String value) {
			this(owner, name, predicate == null ? null : Pattern.compile(predicate, Pattern.CASE_INSENSITIVE), value == null ? null : Pattern.compile(value, Pattern.CASE_INSENSITIVE));
		}

		public Attribute<Attribute<T>> then(String name, String predicate, String value) {
			var attribute = new Attribute<>(this, name, predicate, value);
			if (this.children == null) this.children = new ArrayList<>();
			this.children.add(attribute);
			return attribute;
		}

		public Attribute<T> enableMultipleMatches() {
			this.multipleMatches = true;
			return this;
		}

		public Attribute<T> enableConcatGroups() {
			this.concatGroups = true;
			return this;
		}

		public boolean processResult(String input, Matcher value, Consumer<String> append) {
			String groupValue = null;

			if (this.concatGroups) {
				// Go through all groups and concatenate all those that are not null
				var builder = new StringBuilder();
				for (int i = 1; i <= value.groupCount(); i++) {
					var found = value.group(i);
					if (found != null) {
						if (!builder.isEmpty()) builder.append(" ");
						builder.append(found);
					}
				}

				if (!builder.isEmpty()) groupValue = builder.toString();
			} else {
				// Go through all groups in the value pattern and pick the first one with a value
				for (int i = 1; i <= value.groupCount(); i++) {
					groupValue = value.group(i);
					if (groupValue != null) break;
				}
			}

			if (groupValue != null) {
				append.accept(this.name + ":" + groupValue);

				if (this.children != null) {
					for (var child : this.children) {
						child.match(input, append);
					}
				}

				return true;
			}

			return false;
		}

		public boolean match(String input, Consumer<String> append) {
			// If we have a predicate, it must be in input
			if (this.predicate != null && !this.predicate.matcher(input).find()) return false;

			if (this.value == null) {
				// If we don't have a value pattern, just execute the children then exit
				if (this.children != null) {
					var childrenMatched = false;

					for (var child : this.children) {
						var success = child.match(input, append);
						if (success) childrenMatched = true;
					}

					return childrenMatched;
				}

				return false;
			}

			var value = this.value.matcher(input);
			if (this.multipleMatches) {
				var found = false;

				// Because we allow multiple value, go through all matches
				while (value.find()) {
					var success = this.processResult(input, value, append);
					if (success) found = true;
				}

				return found;
			}

			// Because we don't allow multiple value, go through only the first match
			if (value.find()) {
				return this.processResult(input, value, append);
			}

			return false;
		}
	}

	public static final AttributeExtractor VALUE = new AttributeExtractor();

	static {
		// Match link, handle both [[entity]] and [[entity|label]]
		var linkPattern = "\\[\\[([^|\\]]*?)(?:\\|.*?)?\\]\\]";
		// Match an named entity in a infobox, usually the target of a link, but could also be a plain string which is matched by "= <value>"
		var infoBoxEntityPattern = linkPattern + "|= ([\\w- ]+)";

		// Date that may have a day, month and must have a year. We only care about the year.
		var genericDate = "(?:\\d{1,2} )?(?:[A-Za-z]+ )?(\\d{4})";

		// The unit can be from Hz to GHz. This could exclude Hz, because this is likely to conflict
		// with power input frequency and there are no computers that measure speed in just hertz.
		var clockSpeedUnit = "(?:\\[\\[)?(?:Hertz\\|)?([MGk]Hz)";
		var decimalNumber = "\\d+(?:\\.\\d+)?";
		var clockSpeed = "(" + decimalNumber + ") *(?:&nbsp; *)?" + clockSpeedUnit;

		// Match fields in infoboxes, these are definitely accurate
		VALUE
				.inPattern("(?:^|\\n)\\| *[^\\n=]*?=(?:[^|\\n{\\[]*(?:\\{\\{(?:.*?\\n?)+\\}\\}|\\[\\[.*?\\]\\])? *,?)+")
				.attribute("release", "(?:release\\w*(?: date)?|produced-start) *=", "(\\d{4})").build()
				.attribute("discontinued", "(?:discontinued|produced-end) *=", "(\\d{4})").build()
				.attribute("wordSize", "data-width *=", "(\\d+)").enableMultipleMatches().build()
				.attribute("wordSize", "(?:platform|bits) *=", "(\\d+)-bit").enableMultipleMatches().build()
				.attribute("company", "manuf(?:1|acturer) *=", infoBoxEntityPattern).enableMultipleMatches().build()
				.attribute("company", "(?:developer|designfirm|designer) *=", infoBoxEntityPattern).enableMultipleMatches().build()
				.attribute("company", "owner *=", infoBoxEntityPattern).enableMultipleMatches().build()
				.attribute("company", "soldby *=", infoBoxEntityPattern).enableMultipleMatches().build()
				.attribute("clockSpeed", "(?:cpu|processor|frequency) *=", clockSpeed).enableConcatGroups().enableMultipleMatches().build()
				.attribute("clockSpeed", null, "\\| *slowest *= *(" + decimalNumber + ") *\\| *slow-unit *= *" + clockSpeedUnit).enableConcatGroups().enableMultipleMatches().build()
				.build();

		// Find attributes in plain text directly, these are potentially accurate
		VALUE
				.anywhere()
				.attribute("wordSize?", null, "\\[\\[(\\d+)-bit").build()
				.attribute("wordSize?", null, "(\\w+)-bit (?:word|processor|microprocessor)").build()
				.attribute("clockSpeed?", null, clockSpeed).enableConcatGroups().enableMultipleMatches().build()
				.build();

		// Find attributes in sentences, this is the last resort
		VALUE
				.inPattern("(?<!\\{\\{)[A-Z][a-zA-Z]*(?: ?[^\\n.{\\[ ]+(?:\\.\\d+)?| \\[\\[[^\\]]*?\\]\\]| ?\\{\\{[^}]*?\\}\\})+")
				.allowMultiple()
				// Find manufacturer by finding sentences with "sold/launched/introduced/... by" and then matching the next entity
				.attribute("company?", null, "(?:sold|launched|introduced|developed) (?:.{0,20}? )?by.{0,20}?" + linkPattern)
				// If we found the manufacturer this way, there may also be a release year in the sentence
				.then("release", null, "(?:in|on) " + genericDate).build()
				.build()
				// If there is "released in", collect the mentioned years.
				.attribute(null, "released in", null).then("release?", null, "(?:in|on) " + genericDate).enableMultipleMatches().build().build()
				.build();
	}
}
