package bt7s7k7.vinf_project.cli;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import bt7s7k7.vinf_project.common.Logger;
import bt7s7k7.vinf_project.common.Project;
import bt7s7k7.vinf_project.datasource.Crawler;

public class App {
	public static void main(String[] args) {
		try {
			var rootPath = Paths.get("").toAbsolutePath().resolve("project");
			var project = Project.fromPath(rootPath);

			switch (args.length == 0 ? "" : args[0]) {
				case "crawler" -> {
					var crawler = new Crawler(project,
							// URL to the target site
							new URI("https://gunkies.org/wiki/Main_Page"),
							// Limit crawler to only visit pages on the target site
							"https://gunkies.org/wiki/");

					while (true) {
						var success = crawler.visitNextPage();
						if (!success) {}

						break;
					}

					Logger.success("Finished downloading");
				}
				default -> {
					System.err.println("Invalid arguments");
					System.exit(1);
				}
			}
		} catch (IOException | URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}
}
