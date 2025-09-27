package bt7s7k7.vinf_project.common;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Support {
	private Support() {}

	public static String replaceAll(CharSequence input, Pattern pattern, Function<Matcher, String> replacer) {
		var matcher = pattern.matcher(input);
		var result = new StringBuilder();

		while (matcher.find()) {
			matcher.appendReplacement(result, replacer.apply(matcher));
		}

		matcher.appendTail(result);
		return result.toString();
	}

	public static void matchAll(CharSequence input, Pattern pattern, Consumer<Matcher> callback) {
		var matcher = pattern.matcher(input);
		while (matcher.find()) {
			callback.accept(matcher);
		}
	}

	@FunctionalInterface
	public interface UnsafeFunction<T, R> {
		public R apply(T value) throws Exception;
	}

	public static <T, R> Function<T, R> makeSafe(UnsafeFunction<T, R> unsafeFunction) {
		return value -> {
			try {
				return unsafeFunction.apply(value);
			} catch (Exception e) {
				if (e instanceof RuntimeException runtimeException) throw runtimeException;
				throw new RuntimeException(e);
			}
		};
	}

	public static <T> Iterable<T> toIterable(Iterable<T> iterable) {
		return iterable;
	}
}
