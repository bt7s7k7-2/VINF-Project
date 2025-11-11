package bt7s7k7.vinf_project.cli;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.TerminalBuilder;

import bt7s7k7.vinf_project.common.Logger;
import bt7s7k7.vinf_project.common.Project;
import bt7s7k7.vinf_project.indexing.Indexer;
import bt7s7k7.vinf_project.input.Crawler;
import bt7s7k7.vinf_project.search.SearchEngine;
import bt7s7k7.vinf_project.spark.SparkTest;

public class App {
	public static void main(String[] args) {
		try {
			var rootPath = Paths.get("").toAbsolutePath().resolve("project");
			var project = Project.fromPath(rootPath, new URI("https://gunkies.org/wiki/"));

			switch (args.length == 0 ? "" : args[0]) {
				case "crawler" -> {
					var crawler = new Crawler(project,
							// URL where to start the search
							new URI("https://gunkies.org/wiki/Main_Page"));

					while (true) {
						var success = crawler.visitNextPage();
						if (!success) break;
					}

					Logger.success("Finished downloading");
				}
				case "index" -> {
					var indexer = new Indexer(project);
					indexer.index();
				}
				case "search" -> {
					var searchEngineFuture = CompletableFuture.supplyAsync(() -> {
						try {
							return new SearchEngine(project);
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					});

					var terminal = TerminalBuilder.builder()
							.system(true)
							.build();

					var history = new DefaultHistory();

					var reader = LineReaderBuilder.builder()
							.terminal(terminal)
							.history(history)
							.build();

					while (true) {
						try {
							var line = reader.readLine("> ").trim();
							if (line.isEmpty()) continue;

							var searchEngine = searchEngineFuture.get();
							var suggestions = searchEngine.search(line);
							if (suggestions.isEmpty()) {
								Logger.error("No documents found");
								continue;
							}

							for (var suggestion : suggestions) {
								var document = searchEngine.documentDatabase.findDocumentByIndex(suggestion.document);
								Logger.info("Found: " + document + " \u001b[2m(Score: " + suggestion.score + "; URL: " + project.getAbsoluteURI(document) + ")\u001b[0m");
							}

							Logger.success("Found " + suggestions.size() + " results");
						} catch (UserInterruptException __) {
							continue;
						} catch (EndOfFileException __) {
							break;
						}
					}

					terminal.close();
				}
				case "spark" -> {
					SparkTest.run();
				}
				default -> {
					Logger.error("Invalid arguments");
					System.exit(1);
				}
			}
		} catch (IOException | URISyntaxException | InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}
}
