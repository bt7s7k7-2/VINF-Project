package bt7s7k7.vinf_project;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import bt7s7k7.vinf_project.indexing.TextExtractor;

public class TextExtractorTest {
	@Test
	public void escapeSequence() {
		assertEquals("&", TextExtractor.extractText("&amp;"));
	}

	@Test
	public void basicExtraction() {
		var input = """
				<b>3BSD</b> (called the <b>Third Berkeley Software Distribution</b> in contemporary documentation, generally given as the acronym) was <a href="/wiki/Unix/32V" title="Unix/32V">Unix/32V</a> with a better <a href="/wiki/Virtual_memory" title="Virtual memory">virtual memory</a> system, that went from 32V's <a href="/wiki/Swapping" title="Swapping">swapping</a>, to a paged system.  Additionally all of the BSD tools from <a href="/w/index.php?title=1BSD&amp;action=edit&amp;redlink=1" class="new" title="1BSD (page does not exist)">1BSD</a> &amp; <a href="/w/index.php?title=2BSD&amp;action=edit&amp;redlink=1" class="new" title="2BSD (page does not exist)">2BSD</a> on the <a href="/wiki/PDP-11" title="PDP-11">PDP-11</a> were ported over, including <a href="/wiki/Vi" class="mw-redirect" title="Vi">vi</a>.  I've poorly transcribed the setup documentation here <a href="/wiki/Setting_up_the_Third_Berkeley_Software_Tape" title="Setting up the Third Berkeley Software Tape">Setting up the Third Berkeley Software Tape</a>.  This was the first full fledged OS from Berkley, as the 1&amp;2 releases were just patches to <a href="/wiki/Unix_Seventh_Edition" title="Unix Seventh Edition">V7</a>.
				</p>
				<div id="toc" class="toc"><div class="toctitle"><h2>Contents</h2></div>
				<ul>
				<li class="toclevel-1 tocsection-1"><a href="#How_do_I_get_this_to_run.3F.21"><span class="tocnumber">1</span> <span class="toctext">How do I get this to run?!</span></a></li>
				<li class="toclevel-1 tocsection-2"><a href="#What_Runs.3F"><span class="tocnumber">2</span> <span class="toctext">What Runs?</span></a></li>
				<li class="toclevel-1 tocsection-3"><a href="#Games"><span class="tocnumber">3</span> <span class="toctext">Games</span></a></li>
				<li class="toclevel-1 tocsection-4"><a href="#External_links"><span class="tocnumber">4</span> <span class="toctext">External links</span></a></li>
				</ul>
				</div>""";

		var expected = """
				3BSD (called the Third Berkeley Software Distribution in contemporary documentation, generally given as the acronym) was Unix/32V with a better virtual memory system, that went from 32V's swapping, to a paged system.  Additionally all of the BSD tools from 1BSD & 2BSD on the PDP-11 were ported over, including vi.  I've poorly transcribed the setup documentation here Setting up the Third Berkeley Software Tape.  This was the first full fledged OS from Berkley, as the 1&2 releases were just patches to V7.

				Contents

				1 How do I get this to run?!
				2 What Runs?
				3 Games
				4 External links

				""";

		assertEquals(expected, TextExtractor.extractText(input));
	}

	@Test
	public void scriptAndStyleExtraction() {
		var input = """
				<title>3BSD - Computer History Wiki</title>
				<script>document.documentElement.className = document.documentElement.className.replace( /(^|\s)client-nojs(\s|$)/, "$1client-js$2" );</script>
				<script>(window.RLQ=window.RLQ||[]).push(function(){mw.config.set({"wgCanonicalNamespace":"","wgCanonicalSpecialPageName":false,"wgNamespaceNumber":0,"wgPageName":"3BSD","wgTitle":"3BSD","wgCurRevisionId":32205,"wgRevisionId":32205,"wgArticleId":386,"wgIsArticle":true,"wgIsRedirect":false,"wgAction":"view","wgUserName":null,"wgUserGroups":["*"],"wgCategories":["CSRG BSD"],"wgBreakFrames":false,"wgPageContentLanguage":"en","wgPageContentModel":"wikitext","wgSeparatorTransformTable":["",""],"wgDigitTransformTable":["",""],"wgDefaultDateFormat":"dmy","wgMonthNames":["","January","February","March","April","May","June","July","August","September","October","November","December"],"wgMonthNamesShort":["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],"wgRelevantPageName":"3BSD","wgRelevantArticleId":386,"wgRequestId":"d689731ee922102fecbd65c4","wgIsProbablyEditable":false,"wgRelevantPageIsProbablyEditable":false,"wgRestrictionEdit":[],"wgRestrictionMove":[]});mw.loader.state({"site.styles":"ready","noscript":"ready","user.styles":"ready","user":"ready","user.options":"loading","user.tokens":"loading","mediawiki.legacy.shared":"ready","mediawiki.legacy.commonPrint":"ready","mediawiki.sectionAnchor":"ready","mediawiki.skinning.interface":"ready","mediawiki.skinning.content.externallinks":"ready","skins.monobook.styles":"ready"});mw.loader.implement("user.options@0bhc5ha",function($,jQuery,require,module){mw.user.options.set([]);});mw.loader.implement("user.tokens@0o44f1h",function ( $, jQuery, require, module ) {
				mw.user.tokens.set({"editToken":"+\\","patrolToken":"+\\","watchToken":"+\\","csrfToken":"+\\"});/*@nomin*/

				});mw.loader.load(["site","mediawiki.page.startup","mediawiki.user","mediawiki.hidpi","mediawiki.page.ready","jquery.makeCollapsible","mediawiki.toc","mediawiki.searchSuggest"]);});</script>
				<link rel="stylesheet" href="/w/load.php?debug=false&amp;lang=en&amp;modules=mediawiki.legacy.commonPrint%2Cshared%7Cmediawiki.sectionAnchor%7Cmediawiki.skinning.content.externallinks%7Cmediawiki.skinning.interface%7Cskins.monobook.styles&amp;only=styles&amp;skin=monobook"/>
				<script async="" src="/w/load.php?debug=false&amp;lang=en&amp;modules=startup&amp;only=scripts&amp;skin=monobook"></script>""";

		var expected = """
				3BSD - Computer History Wiki



				""";

		assertEquals(expected, TextExtractor.extractText(input));
	}

	@Test
	public void extractTokens() {
		var input = "The BA11-P card cage from DEC was used in PDP-11/60 to hold the CPU, main memory and device controllers.";
		var expected = "The,BA11-P,card,cage,from,DEC,was,used,in,PDP-11/60,to,hold,the,CPU,main,memory,and,device,controllers";

		assertEquals(expected, TextExtractor.extractTokens(input).collect(Collectors.joining(",")));
	}
}
