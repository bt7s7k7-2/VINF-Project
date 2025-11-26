package bt7s7k7.vinf_project;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import bt7s7k7.vinf_project.spark.ExtractionPipeline;

public class ExtractionPipelineTest {
	@Test
	public void findMostProbableValueTest() {
		assertEquals(2.0, ExtractionPipeline.findMostProbableValue(Stream.of(1.0, 2.0, 2.0, 3.0, 3.0, 4.0)));
	}
}
