package bt7s7k7.vinf_project.indexing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import com.google.common.collect.Lists;

public class TermInfo {
	public static record Location(int document, int frequency) {}

	public final String value;
	public int totalFrequency;
	public final ArrayList<Location> locations = new ArrayList<>();

	public int getDocumentCount() {
		return this.locations.size();
	}

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

	public int getDF() {
		return this.locations.size();
	}

	public int getTF(int document) {
		var index = Collections.binarySearch(Lists.transform(this.locations, Location::document), document);
		if (index < 0) {
			return 0;
		}

		var location = this.locations.get(index);
		return location.frequency;
	}
}
