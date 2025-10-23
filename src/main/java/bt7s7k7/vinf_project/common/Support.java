package bt7s7k7.vinf_project.common;

import java.util.function.Function;

public final class Support {
	private Support() {}

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
}
