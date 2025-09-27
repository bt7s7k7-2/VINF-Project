package bt7s7k7.vinf_project.datasource;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import bt7s7k7.vinf_project.common.Logger;
import bt7s7k7.vinf_project.common.Project;
import bt7s7k7.vinf_project.common.Support;

public class Crawler {
	/** URL to start the crawling process. */
	public final URI entry;

	/**
	 * To ensure the crawler does not visit sites other than the target site, all URLs will be
	 * tested to start with this string.
	 */
	public final String mask;

	public final Project project;
	public final InputFileManager inputFileManager;

	protected LinkedHashSet<URI> pagesToVisit = null;

	public Crawler(Project project, URI entry, String mask) throws IOException {
		this.project = project;
		this.entry = entry;
		this.mask = mask;
		this.inputFileManager = this.project.getInputFileManager();
	}

	public Path getQueueFile() {
		return this.project.rootPath.resolve("crawlerQueue.txt");
	}

	public boolean isUrlPartOfTargetSize(URI url) {
		return url.toString().startsWith(this.mask);
	}

	private static final Pattern LINK_HREF = Pattern.compile("href=\"([^\"]+)\"");

	public boolean visitNextPage() throws IOException {
		if (this.pagesToVisit == null) {
			// If our queue was not initialized yet, initialize it

			var queueFile = this.getQueueFile();
			if (Files.exists(queueFile)) {
				// If there is a queue saved to the filesystem, load it
				this.pagesToVisit = Files.readAllLines(queueFile).stream()
						// Ignore empty lines
						.filter(StringUtils::isNotBlank)
						// Parse every line as a URL
						.map(Support.makeSafe(URI::new))
						// Create a queue object from the parsed URLs
						.collect(Collectors.toCollection(LinkedHashSet::new));
			} else {
				// Otherwise use the default entry
				this.pagesToVisit = new LinkedHashSet<>();
				this.pagesToVisit.add(this.entry);
			}
		}

		if (this.pagesToVisit.isEmpty()) {
			return false;
		}

		// Get the first page in queue
		var page = this.pagesToVisit.getFirst();
		Logger.info("Downloading page: " + page.toString());

		var pageName = getPageName(page);

		// Check if the page has already been downloaded. This should not happen, since we wouldn't
		// add a page to a queue if it already existed, but just in case.
		if (this.inputFileManager.hasFileWithName(pageName)) {
			Logger.warn("Page " + page.toString() + " is already downloaded, skipping");
			// Remove the visited page from the queue
			this.pagesToVisit.remove(page);
			this.saveQueue();
			return true;
		}

		InputFile inputFile;

		// Download the page and save it
		try (var stream = page.toURL().openStream()) {
			var content = stream.readAllBytes();
			var contentText = new String(content, StandardCharsets.UTF_8);
			inputFile = InputFile.fromContent(pageName, contentText);
			this.inputFileManager.addFile(inputFile);
		}

		Support.matchAll(inputFile.content, LINK_HREF, matcher -> {
			var link = matcher.group(1);
			// Ensure the URL is absolute
			var nextPage = page.resolve(link);

			this.queuePageIfValid(nextPage);
		});

		// Remove the visited page from the queue
		this.pagesToVisit.remove(page);
		this.saveQueue();
		Logger.success("Done. " + this.pagesToVisit.size() + " pages in queue");
		return true;
	}

	public void queuePageIfValid(URI nextPage) {
		// Check if the page is not external
		if (!this.isUrlPartOfTargetSize(nextPage)) return;

		// Check if we already have this page
		var nextPageName = getPageName(nextPage);
		if (this.inputFileManager.hasFileWithName(nextPageName)) {
			return;
		}

		// Ignore pages not related to content
		if (nextPageName.startsWith("User:")
				|| nextPageName.startsWith("Help:")
				|| nextPageName.startsWith("Template:")
				|| nextPageName.startsWith("Talk:")
				|| nextPageName.startsWith("User_talk:")) {
			return;
		}

		// Check if the page is queued
		if (this.pagesToVisit.contains(nextPage)) return;

		// Queue the page
		this.pagesToVisit.add(nextPage);
	}

	public void saveQueue() throws IOException {
		// Convert all queued URLs to string and save them to a file
		Files.write(this.getQueueFile(), Support.toIterable(this.pagesToVisit.stream().map(URI::toString)::iterator));
	}

	private static String getPageName(URI page) {
		return Paths.get(page.getPath()).getFileName().toString();
	}
}
