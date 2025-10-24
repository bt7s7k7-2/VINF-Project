package bt7s7k7.vinf_project.common;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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

	public static <T> List<T> getIntersection(List<T> list1, List<T> list2, Comparator<T> compare) {
		var result = new ArrayList<T>();

		var index1 = 0;
		var index2 = 0;

		while (index1 < list1.size() && index2 < list2.size()) {
			var a = list1.get(index1);
			var b = list2.get(index2);

			if (compare.compare(a, b) == 0) {
				result.add(a);
				index1++;
				index2++;
			} else {
				if (compare.compare(a, b) < 0) {
					index1++;
				} else {
					index2++;
				}
			}
		}

		return result;
	}
}
