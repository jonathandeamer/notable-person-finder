#!/usr/bin/env python3
"""Deterministic reliable-source filter for Gate 4.

Reads state/brave_coverage.jsonl and keeps only results whose URL comes from
a domain Wikipedia considers a reliable source.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from urllib import parse as urlparse


# ---------------------------------------------------------------------------
# Wikipedia reliable-source domain list
# Entries may be:
#   - plain domain          "bbc.co.uk"          → exact or subdomain match
#   - domain + path prefix  "atlasobscura.com/articles" → URL prefix match
#   - full https:// URL     "https://open.spotify.com/..." → URL prefix match
# ---------------------------------------------------------------------------
RELIABLE_ENTRIES: list[str] = [
    "abcnews.com",
    "abcnews.go.com",
    "adl.org",
    "adl.org/resources/hate-symbols/search",
    "afp.com",
    "aljazeera.com",
    "aljazeera.net",
    "amnesty.org",
    "aon.com",
    "ap.org",
    "apnews.com",
    "arstechnica.co.uk",
    "arstechnica.com",
    "atlasobscura.com/articles",
    "avclub.com",
    "avn.com",
    "axios.com",
    "ballotpedia.org",
    "bbc.co.uk",
    "bbc.com",
    "behindthevoiceactors.com",
    "bellingcat.com",
    "bloomberg.com",
    "burkespeerage.com",
    "businessweek.com",
    "buzzfeed.com",
    "buzzfeednews.com",
    "cbsnews.com",
    "channelnewsasia.com",
    "checkyourfact.com",
    "climatefeedback.org",
    "cnet.com",
    "cnn.com",
    "codastory.com",
    "commonsensemedia.org",
    "csmonitor.com",
    "deadline.com",
    "deadlinehollywooddaily.com",
    "debretts.com",
    "denofgeek.com",
    "deseret.com",
    "deseretnews.com",
    "digitalspy.co.uk",
    "digitalspy.com",
    "digitaltrends.com",
    "dw.com/en",
    "economist.com",
    "engadget.com",
    "eurogamer.cz",
    "eurogamer.de",
    "eurogamer.es",
    "eurogamer.net",
    "eurogamer.nl",
    "eurogamer.pl",
    "eurogamer.pt",
    "ew.com",
    "forbes.com",
    "freebeacon.com",
    "ft.com",
    "gamasutra.com",
    "gamedeveloper.com",
    "gameinformer.com",
    "gamerankings.com",
    "gamespot.co.uk",
    "gamespot.com",
    "geonames.usgs.gov",
    "gizmodo.com",
    "glaad.org",
    "gq-magazine.co.uk",
    "gq.com",
    "grubstreet.com",
    "guardian.co.uk",
    "haaretz.co.il",
    "haaretz.com",
    "hardcoregaming101.net",
    "hollywoodreporter.com",
    "https://open.spotify.com/show/6D4W8XJFVJ15tvqAnbmwJH",
    "huffingtonpost.ca",
    "huffingtonpost.co.uk",
    "huffingtonpost.com",
    "huffingtonpost.com.au",
    "huffingtonpost.com.mx",
    "huffingtonpost.de",
    "huffingtonpost.es",
    "huffingtonpost.fr",
    "huffingtonpost.gr",
    "huffingtonpost.in",
    "huffingtonpost.it",
    "huffingtonpost.jp",
    "huffingtonpost.kr",
    "huffpost.com",
    "huffpostbrasil.com",
    "huffpostmaghreb.com",
    "idolator.com",
    "ifcncodeofprinciples.poynter.org",
    "ign.com",
    "independent.co.uk",
    "indianexpress.com",
    "insider.com",
    "ipscuba.net",
    "ipsnews.net",
    "ipsnoticias.net",
    "iranicaonline.org",
    "jamanetwork.com",
    "journalism.org",
    "jpost.com",
    "kirkusreviews.com",
    "kommersant.com",
    "kommersant.ru",
    "kommersant.uk",
    "latimes.com",
    "lwn.net",
    "meduza.io",
    "metacritic.com",
    "metro.co.uk",
    "metro.news",
    "mg.co.za",
    "monde-diplomatique.fr",
    "mondediplo.com",
    "motherjones.com",
    "msnbc.com",
    "nationalgeographic.com",
    "nationalpost.com",
    "nbcnews.com",
    "newrepublic.com",
    "news.sky.com",
    "news.yahoo.com",
    "newslaundry.com",
    "newsnationnow.com",
    "newsweek.com",
    "newyorker.com",
    "nme.com",
    "npr.org",
    "nydailynews.com",
    "nymag.com",
    "nytimes.com",
    "nzherald.co.nz",
    "oko.press",
    "pbs.org",
    "people-press.org",
    "people.com",
    "pewforum.org",
    "pewglobal.org",
    "pewhispanic.org",
    "pewinternet.org",
    "pewresearch.org",
    "pewsocialtrends.org",
    "pinknews.co.uk",
    "playboy.com",
    "politico.com",
    "politifact.com",
    "polygon.com",
    "propublica.org",
    "rappler.com",
    "reason.com",
    "reuters.com",
    "rfa.org",
    "rollingstone.com",
    "rottentomatoes.com",
    "scientificamerican.com",
    "scmp.com",
    "scotusblog.com",
    "si.com",
    "sixthtone.com",
    "skepticalinquirer.org",
    "smh.com.au",
    "snopes.com",
    "space.com",
    "spiegel.de",
    "splcenter.org",
    "straitstimes.com",
    "telegraph.co.uk",
    "theage.com.au",
    "theatlantic.com",
    "theaustralian.com.au",
    "theconversation.com",
    "thecut.com",
    "thediplomat.com",
    "theglobeandmail.com",
    "theguardian.co.uk",
    "theguardian.com",
    "thehill.com",
    "thehindu.com",
    "theinsneider.com",
    "theintercept.com",
    "themarysue.com",
    "thenation.com",
    "theregister.co.uk",
    "theregister.com",
    "thesundaytimes.co.uk",
    "thetimes.co.uk",
    "thetimes.com",
    "theverge.com",
    "thewire.in",
    "thewirehindi.com",
    "thewireurdu.com",
    "thewrap.com",
    "thisisinsider.com",
    "time.com",
    "timesofisrael.com",
    "timesonline.co.uk",
    "torrentfreak.com",
    "tvguide.com",
    "tvguidemagazine.com",
    "usatoday.com",
    "usatoday.com/story/special/contributor-content",
    "usgamer.net",
    "usnews.com",
    "vanityfair.com",
    "variety.com",
    "villagevoice.com",
    "voanews.com",
    "vogue.com",
    "vox.com",
    "vulture.com",
    "washingtonpost.com",
    "weeklystandard.com",
    "wired.co.uk",
    "wired.com",
    "wsj.com",
    "wyborcza.pl",
    "zdnet.com",
]


def is_reliable_source(url: str, source_domain: str) -> bool:
    """Return True if the result URL comes from a Wikipedia reliable source."""
    for entry in RELIABLE_ENTRIES:
        if entry.startswith("https://") or entry.startswith("http://"):
            # Full URL prefix match
            if url.startswith(entry):
                return True
        elif "/" in entry:
            # Domain + path prefix: match against full URL with or without www.
            for scheme in ("https://", "http://"):
                if url.startswith(f"{scheme}{entry}") or url.startswith(
                    f"{scheme}www.{entry}"
                ):
                    return True
        else:
            # Plain domain: exact match or subdomain (e.g. uk.news.yahoo.com)
            if source_domain == entry or source_domain.endswith("." + entry):
                return True
    return False


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Filter Brave coverage results to Wikipedia reliable sources"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("state/brave_coverage.jsonl"),
        help="Input JSONL (Gate 4 Brave coverage output)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("state/gate4_reliable_coverage.jsonl"),
        help="Output JSONL with results filtered to reliable sources",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=5,
        help="Print progress every N records",
    )
    return parser


def _read_jsonl(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                print(f"warning: invalid JSON at line {line_no}; skipping")
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def run(
    input_path: Path,
    output_path: Path,
    overwrite: bool,
    progress_every: int,
) -> int:
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"output exists: {output_path} (use --overwrite)")

    rows = _read_jsonl(input_path)
    total = len(rows)
    filter_counts: Counter = Counter()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as out_f:
        for idx, row in enumerate(rows, start=1):
            all_results = row.get("brave_results") or []
            reliable = [
                r for r in all_results
                if is_reliable_source(r.get("url", ""), r.get("source_domain", ""))
            ]
            # Re-rank
            for new_rank, r in enumerate(reliable, start=1):
                r["rank"] = new_rank

            kept = len(reliable)
            dropped = len(all_results) - kept
            filter_counts["kept"] += kept
            filter_counts["dropped"] += dropped

            out_row = dict(row)
            out_row["brave_results"] = reliable
            out_row["brave_result_count"] = kept
            out_row["brave_result_count_unfiltered"] = len(all_results)

            out_f.write(json.dumps(out_row, ensure_ascii=False, sort_keys=True) + "\n")
            out_f.flush()

            if progress_every and idx % progress_every == 0:
                print(f"progress: {idx}/{total}")

    print(f"records_written: {total}")
    print(f"results_kept: {filter_counts['kept']}")
    print(f"results_dropped: {filter_counts['dropped']}")
    print(f"output: {output_path}")
    return 0


def main() -> int:
    args = build_arg_parser().parse_args()
    return run(
        input_path=args.input,
        output_path=args.output,
        overwrite=args.overwrite,
        progress_every=args.progress_every,
    )


if __name__ == "__main__":
    raise SystemExit(main())
