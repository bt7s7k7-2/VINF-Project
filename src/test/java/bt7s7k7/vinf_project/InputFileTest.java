package bt7s7k7.vinf_project;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import bt7s7k7.vinf_project.datasource.InputFile;

public class InputFileTest {
	@Test
	public void nameConversion() {
		assertEquals("Special(3a)Name.html", InputFile.nameToFilename("Special:Name"));
		assertEquals("Special:Name", InputFile.filenameToName("Special(3a)Name.html"));
	}

	@Test
	public void equality() {
		// Prevent string interning
		var name1 = String.join("", List.of("n", "a", "m", "e"));
		var name2 = String.join("", List.of("n", "a", "m", "e"));

		assertFalse(name1 == name2);

		var file1 = InputFile.fromContent(name1, "a");
		var file2 = InputFile.fromContent(name2, "b");
		var file3 = InputFile.fromContent("other", "b");

		assertFalse(file1 == file2);
		assertEquals(file1, file2);

		var set = new HashSet<InputFile>();
		set.add(file1);
		assertTrue(set.contains(file2));
		assertFalse(set.contains(file3));
	}
}
