package bt7s7k7.vinf_project.indexing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class TermInfo {
	public static record Location(int document, int frequency) {}

	public final String value;
	public int totalFrequency;
	public final ArrayList<Location> locations = new ArrayList<>();

	public TermInfo(String value, int totalFrequency) {
		this.value = value;
		this.totalFrequency = totalFrequency;
	}

	public void addLocation(int document, int frequency) {
		var location = new Location(document, frequency);
		// Find the index of the location using binary search to maintain the ordering of the locations by their document
		var index = Collections.binarySearch(this.locations, location, Comparator.comparing(Location::document));
		// The returned index must be < 0, because the location with the same document ID cannot exist
		assert index < 0;

		index = -index - 1;
		this.locations.add(index, location);

		this.totalFrequency += frequency;
	}
}
