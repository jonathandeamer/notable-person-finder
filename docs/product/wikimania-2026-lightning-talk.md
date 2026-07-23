# Who's Missing? Using AI to Spot Wikipedia's Coverage Gaps

**Event:** Wikimania 2026  
**Session type:** Lightning talk Showcase  
**Track:** Artificial intelligence  
**Speaker:** Jonathan Deamer  
**Date delivered:** 2026-07-23

## Speech

Good afternoon.

I'm Jonathan, and I'm an English-language Wikipedia editor, based in Liverpool, UK. Today I’m going to talk about using AI to find notable people who are missing Wikipedia biographies, while keeping the writing and editorial judgment entirely human.

I write articles about anything that sparks my curiosity: often biographies of contemporary artists or figures from local history.

Most biographies I've written have started with the serendipity of me reading a magazine or newspaper, going to check the Wikipedia page, and having that joyful moment that every editor loves: there's a new page to write!

But there's more news published every day than anyone can keep up with. Somewhere in that news is someone I might care about who’s now notable enough for Wikipedia. I'd never know, unless I spent every day trawling RSS feeds, tallying up coverage, and checking who already has an article.

Of course, sometimes Wikipedia is missing an article because the sourcing is simply not there. But sometimes the sources are there, and the missing ingredient is that no editor happened to read that obituary, or they assumed there must already be an encyclopedia page for this person.

**Next slide (tool flow diagram)**

So I built a small tool that uses AI to help discover these gaps.

It watches news sources chosen by an editor, looks for named people, checks whether English Wikipedia has an article about them, and produces a shortlist of people who likely now meet notability criteria. The editor can then determine whether to write an article.

The AI is used only at carefully defined decision gates, rather than being given free rein to generate content. For example: is this news story really about the same person as an existing encyclopedia article? Or: five people are mentioned in a story - who's really its subject, and who's just mentioned in passing. Those are trivial tasks for a person, but difficult to automate at scale, and that’s one way underrepresentation happens.

Importantly, the tool never edits Wikipedia. The human editor still reads the sources, decides whether the case is strong enough under our normal policies, writes and cites the article, and puts it through the same community processes as any other new page.

The first article I wrote this way was about Ray Mouton, an American lawyer known for his role in Catholic church abuse litigation. The tool spotted a New York Times obituary for someone without a Wikipedia article, and revealed that reliable sources had existed for years - but no one had connected the dots. I wrote the article myself, and it was later featured on English Wikipedia's Main Page after the usual community review.

The useful role the AI played was that of paying attention: noticing something I might otherwise have missed.

Skip if after 3:30:
(The approach is reusable because every editor has different sources they care about. Some Wikiprojects already do this by hand, compiling lists of missing biographies from subject-specific dictionaries and databases. A tool like this can sit alongside that work, watching sources that rarely reach global front pages.)

**Switch to final slide**

The code is open source if you want to try it, but the exact software is less important than the pattern: let machines help with monitoring and triage, enabling humans to do the work that requires judgment.

I plan on developing this tool further. I’d love this to become a community resource, where editors can subscribe to receive notifications of articles they could write on topics that interest them, with reliable sources already found.  

Wikipedia runs on volunteer attention, and it’s the scarcest resource we have. If AI can do a little more of the paying attention, then editors can spend more time doing what only people can do: exercising judgment, writing articles, and collaborating with each other. Thank you.

(and please be in touch if you want to talk about AI optimism, in Wikimedia projects and beyond).
