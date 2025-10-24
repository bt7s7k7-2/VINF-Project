package bt7s7k7.vinf_project.input;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.net.URIBuilder;

import bt7s7k7.vinf_project.common.Logger;
import bt7s7k7.vinf_project.common.Project;
import bt7s7k7.vinf_project.common.Support;

public class Crawler {
	/** URL to start the crawling process. */
	public final URI entry;

	public final Project project;
	public final InputFileManager inputFileManager;

	// Utilizing a LinkedHashSet allows to efficiently look up, if a page is already queued, while
	// simultaneously providing an ordered queue with its linked-list functionality.
	protected LinkedHashSet<URI> pagesToVisit = null;

	protected CloseableHttpClient httpClient = HttpClientBuilder.create()
			// Disable redirect handling, we want to handle it manually to prevent duplicate
			// downloading when redirecting to an already downloaded page
			.disableRedirectHandling()
			.setUserAgent("Web crawler for VINF course at FIIT STU, contact me at branislav.trstensky@gmail.com for complaints")
			.build();

	public Crawler(Project project, URI entry) throws IOException {
		this.project = project;
		this.entry = entry;
		this.inputFileManager = this.project.getInputFileManager();
	}

	public Path getQueueFile() {
		return this.project.rootPath.resolve("crawlerQueue.txt");
	}

	// Because the target site is a MediaWiki site, all links are created automatically from
	// MediaWiki syntax and so can assume that all links will have standard syntax.
	private static final Pattern LINK_HREF = Pattern.compile("href=\"([^\"]+)\"");

	public boolean visitNextPage() throws IOException, URISyntaxException, InterruptedException {
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

		// If there are not pages in the queue, exit
		if (this.pagesToVisit.isEmpty()) {
			return false;
		}

		// Get the first page in queue
		var page = this.pagesToVisit.getFirst();
		var pageName = this.getPageName(page);
		Logger.info("Downloading page: " + pageName + " (" + page.toString() + ")");

		do {
			// Check if the page has already been downloaded. This should not happen, since we wouldn't
			// add a page to a queue if it already existed, but just in case.
			if (this.inputFileManager.hasFileWithName(pageName)) {
				Logger.warn("Page " + pageName + " (" + page.toString() + ") is already downloaded, skipping");
				break;
			}

			// Test if this page is not valid, it may have been added to the queue in a previous version
			// of this program with a different filter.
			if (!this.isPageValid(page)) {
				Logger.warn("Page " + pageName + " (" + page.toString() + ") is not valid, skipping");
				break;
			}

			InputFile inputFile;

			// Request the page
			var response = (ClassicHttpResponse) Request.get(page)
					.execute(this.httpClient)
					.returnResponse();

			// If the request was successful save it
			if (response.getCode() == 200) {
				var content = response.getEntity().getContent().readAllBytes();
				var contentText = new String(content, StandardCharsets.UTF_8);
				inputFile = InputFile.fromContent(pageName, contentText);
				this.inputFileManager.addFile(inputFile);

				// Find all links and add them to the queue
				for (var matcher : (Iterable<MatchResult>) LINK_HREF.matcher(inputFile.content).results()::iterator) {
					var link = matcher.group(1);
					// Ensure the URL is absolute
					var nextPage = page.resolve(link);

					this.queuePageIfValid(nextPage);
				}

				break;
			}

			// Test if we were redirected, if so, add the new page to the queue
			var redirect = response.getFirstHeader("location");
			if (redirect != null) {
				var redirectURL = page.resolve(redirect.getValue());
				Logger.warn("Redirection to: " + redirectURL);
				this.queuePageIfValid(redirectURL);

				break;
			}

			// Failed to load the page, log error
			Logger.error("Received status " + response.getCode() + " for page " + pageName + " (" + page.toString() + ")");
		} while (false);

		// Remove the visited page from the queue
		this.pagesToVisit.remove(page);
		this.saveQueue();
		Logger.text(this.pagesToVisit.size() + " pages in queue");

		// Politeness
		Thread.sleep(Duration.ofSeconds(5));
		return true;
	}

	public boolean isPageValid(URI nextPage) {
		// Check if the page is not external
		if (!this.project.isUrlPartOfTargetSize(nextPage)) return false;

		// Check if the link has a fragment
		if (StringUtils.isNotEmpty(nextPage.getFragment())) return false;

		var urlString = nextPage.toString();
		// Ignore user contribution pages
		if (urlString.contains("Special:Contributions")
				|| urlString.contains("Special:WhatLinksHere")
				|| urlString.contains("Special:RecentChangesLinked")
				|| urlString.contains("Special:Version")) {
			return false;
		}

		// Ignore pages not related to content
		var nextPageName = this.getPageName(nextPage);
		if (nextPageName.startsWith("User:")
				|| nextPageName.startsWith("Help:")
				|| nextPageName.startsWith("Template:")
				|| nextPageName.startsWith("Talk:")
				|| nextPageName.startsWith("File:")
				|| nextPageName.startsWith("MediaWiki:")
				|| nextPageName.startsWith("User_talk:")) {
			return false;
		}

		return true;
	}

	public void queuePageIfValid(URI nextPage) throws URISyntaxException {
		// If the link has a fragment, remove the fragment
		if (StringUtils.isNotEmpty(nextPage.getFragment())) {
			nextPage = new URIBuilder(nextPage)
					.setFragment(null)
					.build();
		}

		// Check if the page is valid
		if (!this.isPageValid(nextPage)) return;

		// Check if we already have this page
		var nextPageName = this.getPageName(nextPage);
		if (this.inputFileManager.hasFileWithName(nextPageName)) {
			return;
		}

		// Check if the page is queued
		if (this.pagesToVisit.contains(nextPage)) return;

		// Queue the page
		this.pagesToVisit.add(nextPage);
	}

	public void saveQueue() throws IOException {
		// Convert all queued URLs to string and save them to a file
		Files.write(this.getQueueFile(), (Iterable<String>) this.pagesToVisit.stream().map(URI::toString)::iterator);
	}

	public String getPageName(URI page) {
		return this.project.getRelativeUri(page).toString();
	}
}
