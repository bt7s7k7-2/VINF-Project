package bt7s7k7.vinf_project;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;

import bt7s7k7.vinf_project.common.Support;

public class SearchTest {
	@Test
	public void intersect() {
		assertIterableEquals(List.of(), Support.getIntersection(List.of(0), List.of(1), Comparator.naturalOrder()));
		assertIterableEquals(List.of(1), Support.getIntersection(List.of(0, 1), List.of(1), Comparator.naturalOrder()));
		assertIterableEquals(List.of(1, 10), Support.getIntersection(List.of(0, 1, 2, 10), List.of(1, 10), Comparator.naturalOrder()));
	}
}
