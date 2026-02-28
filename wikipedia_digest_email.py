#!/usr/bin/env python3
"""
Wikipedia Biographical Digest + Obituary Digest — v9.4

Sections:
  1. Wikipedia On This Day — 4 biographical entries with labelled snippets
  2. Obituary Digest — 4 recent obituaries (2 NYT + 2 Guardian) via RSS,
     with archive.ph / Wayback / original URL / og:description fallback

v9 changes from v8.1:
  - Randomisation: shuffle so repeated runs on the same date vary
  - Obituary section: RSS feeds → archive resolution → scoring → 5-6 sentence teaser
  - Fault-tolerant: obituary failures don't block the Wikipedia section

v9.1 changes — scoring refinements based on user's editorial taste:
  - 5 new secondary signal categories:
      direct_quote      — detects sentences containing the subject's own words
      chance_encounter  — pivotal moments of serendipity ("overheard", "one day", "wrong turn")
      late_bloom        — figures overlooked for years then discovered late in life
      origin_story      — humble beginnings, garage workshops, family formative moments
      vivid_detail      — eccentric concrete details (coffins, hidden drawings, daily rituals)
  - Teaser extraction now gives a strong bonus to sentences with direct quotes (score +2.0),
    speech attribution (+1.0), vivid personal detail, and origin story context
  - Expanded: diy, defiance, heretic, humanising, irony signal patterns
  - New SIGNAL_LABELS for all five new categories

v9.2 changes — seen-items persistence:
  - seen_items.json tracks sent Wikipedia titles and obituary URLs (90-day window)
  - load_seen() / save_seen() utilities
  - GitHub Actions YAML writes back seen_items.json after each run

v9.3 changes — digest quality improvements based on review of Feb 25 & Feb 27 digests:
  - Year extraction extended to ancient figures (pre-1000 AD); Constantine now shows
    "(272–337)" instead of "(272–present)"
  - JavaScript code leaked from Guardian <p> tags stripped via _is_js_contaminated()
    and _JS_CONTAMINATION_PATTERNS filter applied in _strip_html_tags()
  - og:description meta-tag extracted from NYT page <head> as step-4 fallback when
    archive content < 500 chars; provides rich editorial lede even behind paywall
  - Sentence fragments filtered from Wikipedia bio snippets via _is_sentence_fragment();
    sentences starting mid-thought (lowercase first char, or continuation words
    like "and/but/however/his/her/with/…") are excluded
  - Duplicate section labels prevented in extract_anecdote() via used_labels dict;
    second occurrence becomes "Label (II)", third "Label (III)", etc.
  - Randomisation strengthened from uniform(0, 3) to uniform(0, 8) so day-to-day
    and intra-day runs produce meaningfully different selections

v9.4 changes — narrative teaser quality and NYT link fix:
  - _strip_html_tags() now joins <p> tags with \\n\\n instead of a single space,
    preserving paragraph structure for downstream teaser extraction
  - _extract_teaser() rewritten with a paragraph-first strategy: when ≥3 paragraphs
    are present, picks the 3 highest-scoring paragraphs and returns them joined by
    \\n\\n (vs. the old scattered-sentence approach). Falls back to sentence-level
    extraction for flat text (og:description, RSS descriptions)
  - _score_text_block() helper consolidates all bonuses (quote, vivid, origin, position)
    and is shared by both the paragraph and sentence extraction paths
  - _obituary_card() renders multi-paragraph teasers as separate <p> elements,
    giving the flowing editorial feel the user demonstrated with Jean Wilson's obit
  - NYT archive_url is always forced to https://archive.ph/newest/{nyt_url},
    ensuring the "Read the full obituary" link bypasses the paywall for the reader
"""

import sys
import os
import re
import json
import time
import random
import logging
import datetime
import smtplib
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parsedate_to_datetime

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

# Gmail SMTP config — set via GitHub Secrets
GMAIL_EMAIL        = os.environ.get("GMAIL_EMAIL", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
RECIPIENT          = os.environ.get("DIGEST_RECIPIENT", "")

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

HEADERS = {
    "User-Agent": "WikipediaBiographicalDigest/9.0 (personal digest; private user)",
    "Accept": "application/json",
}

# Seen-items file — lives alongside the script in the repo root.
# The GitHub Actions workflow commits it back after each run so
# the same bio or obituary is never sent twice.
_SEEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seen_items.json")
_SEEN_RETENTION_DAYS = 90   # entries older than this are pruned


# ─────────────────────────────────────────────────────────────────────────────
# Seen-items persistence  (v9.2)
# ─────────────────────────────────────────────────────────────────────────────

def load_seen() -> dict:
    """
    Load the seen-items registry from disk.
    Returns a dict:
        {
          "wikipedia":  [{"title": str, "date": "YYYY-MM-DD"}, ...],
          "obituaries": [{"url": str, "name": str, "date": "YYYY-MM-DD"}, ...]
        }
    Creates an empty registry if the file doesn't exist yet.
    """
    if not os.path.exists(_SEEN_FILE):
        log.info("No seen_items.json found — starting fresh.")
        return {"wikipedia": [], "obituaries": []}
    try:
        with open(_SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        log.info(
            "Loaded seen_items.json — %d Wikipedia, %d obituary entries",
            len(data.get("wikipedia", [])),
            len(data.get("obituaries", [])),
        )
        return data
    except Exception as exc:
        log.warning("Could not read seen_items.json (%s) — starting fresh.", exc)
        return {"wikipedia": [], "obituaries": []}


def save_seen(seen: dict, new_wiki_titles: list, new_obit_urls: list) -> None:
    """
    Append newly-sent items to the registry and prune entries older than
    _SEEN_RETENTION_DAYS days, then write back to disk.
    """
    today_str = datetime.date.today().isoformat()
    cutoff    = (
        datetime.date.today() - datetime.timedelta(days=_SEEN_RETENTION_DAYS)
    ).isoformat()

    # Append new Wikipedia titles
    for title in new_wiki_titles:
        seen["wikipedia"].append({"title": title, "date": today_str})

    # Append new obituary URLs
    for url, name in new_obit_urls:
        seen["obituaries"].append({"url": url, "name": name, "date": today_str})

    # Prune old entries
    seen["wikipedia"]  = [e for e in seen["wikipedia"]  if e.get("date", "") >= cutoff]
    seen["obituaries"] = [e for e in seen["obituaries"] if e.get("date", "") >= cutoff]

    try:
        with open(_SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(seen, f, indent=2, ensure_ascii=False)
        log.info(
            "saved seen_items.json — %d Wikipedia, %d obituary entries",
            len(seen["wikipedia"]),
            len(seen["obituaries"]),
        )
    except Exception as exc:
        log.error("Could not write seen_items.json: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helper
# ─────────────────────────────────────────────────────────────────────────────

def http_get_json(url: str, retries: int = 3, delay: float = 2.0):
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read().decode("utf-8", errors="replace"))
        except Exception as exc:
            log.warning("Attempt %d/%d failed for %s: %s", attempt, retries, url, exc)
            if attempt < retries:
                time.sleep(delay)
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Fetch real people from Wikipedia's On This Day REST API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_candidates(month_name: str, month_num: str, day: str) -> list:
    """
    Fetch person candidates for today's date.
    Primary: Wikipedia REST API (official numeric month format).
    Fallback: Wikipedia Action API parsing the date article's Births/Deaths sections.
    """
    candidates = []
    seen = set()

    rest_succeeded = False
    for category in ("births", "deaths"):
        url = (
            f"https://en.wikipedia.org/api/rest_v1/feed/onthisday"
            f"/{category}/{month_num}/{day}"
        )
        data = http_get_json(url)
        entries = data.get(category, [])
        log.info("REST API — %s: %d raw entries", category, len(entries))

        if entries:
            rest_succeeded = True

        for entry in entries:
            year  = entry.get("year")
            pages = entry.get("pages", [])
            for page in pages:
                title       = page.get("title", "").strip()
                description = page.get("description", "").strip()
                if not title or title in seen:
                    continue
                if not _is_person(title, description):
                    continue
                seen.add(title)
                candidates.append({
                    "name":        page.get("normalizedtitle", title),
                    "title":       title,
                    "description": description,
                    "birth_year":  "?",
                    "death_year":  "present",
                    "api_year":    year,
                    "source":      category,
                })
        time.sleep(0.5)

    if rest_succeeded:
        log.info("REST API succeeded — %d person candidates", len(candidates))
        return candidates

    log.warning("REST API returned no data; falling back to Action API date article")
    day_unpadded = str(int(day))
    candidates = _fallback_from_date_article(month_name, day_unpadded, seen)

    log.info("Total confirmed person candidates: %d", len(candidates))
    return candidates


def _fallback_from_date_article(month_name: str, day: str, seen: set) -> list:
    """
    Fetch the main Wikipedia article for the date (e.g. 'February_25'),
    extract the Births and Deaths sections, and return person candidates.
    """
    candidates = []
    date_title = f"{month_name}_{day}"

    params = urllib.parse.urlencode({
        "action":        "query",
        "titles":        date_title,
        "prop":          "revisions",
        "rvprop":        "content",
        "rvslots":       "main",
        "format":        "json",
        "formatversion": "2",
    })
    data = http_get_json(f"{WP_API}?{params}")
    pages = data.get("query", {}).get("pages", [])
    if not pages or pages[0].get("missing"):
        log.error("Could not fetch date article: %s", date_title)
        return candidates

    wikitext = (
        pages[0]
        .get("revisions", [{}])[0]
        .get("slots", {})
        .get("main", {})
        .get("content", "")
    )

    link_pattern = re.compile(r'\[\[([^\|\]#]+)(?:\|[^\]]+)?\]\]')
    year_pattern  = re.compile(r'^\*\s*(\d{4})')

    in_section   = False
    section_type = "births"

    for line in wikitext.splitlines():
        lower = line.lower().strip()

        if re.match(r'==\s*births\s*==', lower):
            in_section   = True
            section_type = "births"
            continue
        elif re.match(r'==\s*deaths\s*==', lower):
            in_section   = True
            section_type = "deaths"
            continue
        elif re.match(r'==\s*\w', lower) and in_section:
            in_section = False
            continue

        if not in_section:
            continue

        year_m = year_pattern.match(line)
        year   = int(year_m.group(1)) if year_m else None

        links = link_pattern.findall(line)
        for linked in links:
            linked = linked.strip()
            if re.match(r'^\d+$', linked):
                continue
            if linked in seen:
                continue
            if not _is_person(linked, ""):
                continue
            seen.add(linked)
            candidates.append({
                "name":        linked,
                "title":       linked,
                "description": "",
                "birth_year":  "?",
                "death_year":  "present",
                "api_year":    year,
                "source":      section_type,
            })

    log.info("Fallback Action API — %d candidates from date article", len(candidates))
    return candidates


_PERSON_SIGNALS = [
    r'\bactor\b', r'\bactress\b', r'\bauthor\b', r'\bwriter\b', r'\bpoet\b',
    r'\bnovelist\b', r'\bplaywright\b', r'\bjournalist\b', r'\beditor\b',
    r'\bpolitician\b', r'\bpresident\b', r'\bprime minister\b', r'\bsenator\b',
    r'\bking\b', r'\bqueen\b', r'\bprince\b', r'\bprincess\b', r'\bmonarch\b',
    r'\bemperor\b', r'\bempress\b', r'\btsarina?\b', r'\bpharaoh\b',
    r'\bgeneral\b', r'\badmiral\b', r'\bcolonel\b', r'\bcommander\b',
    r'\bscientist\b', r'\bphysicist\b', r'\bchemist\b', r'\bbiologist\b',
    r'\bmathematician\b', r'\bastronomer\b', r'\bgeologist\b',
    r'\bphilosopher\b', r'\btheologian\b', r'\barchbishop\b', r'\bbishop\b',
    r'\bcomposer\b', r'\bmusician\b', r'\bsinger\b', r'\bpianist\b',
    r'\bpainter\b', r'\bartist\b', r'\bsculptor\b', r'\barchitect\b',
    r'\bphotographer\b', r'\bdirector\b', r'\bproducer\b', r'\bfilmmaker\b',
    r'\binventor\b', r'\bengineer\b', r'\bexplorer\b', r'\baviator\b',
    r'\bpilot\b', r'\bastronaut\b', r'\bcosmonaut\b',
    r'\bathlete\b', r'\bfootballer\b', r'\bboxer\b', r'\bcricketer\b',
    r'\btennis player\b', r'\bcyclist\b', r'\bswimmer\b', r'\bjockey\b',
    r'\beconomist\b', r'\bpsychologist\b', r'\bsociologist\b',
    r'\bhistorian\b', r'\barchaeologist\b', r'\banthropologist\b',
    r'\bmagician\b', r'\bcomedian\b', r'\bhumorist\b', r'\bsatirist\b',
    r'\bactivist\b', r'\breformer\b', r'\brevolutionary\b', r'\bdissident\b',
    r'\bnobel\b', r'\bborn\b', r'\bdied\b',
    r'\bamerican\b', r'\bbritish\b', r'\benglish\b', r'\bscottish\b',
    r'\birish\b', r'\bwelsh\b', r'\bfrench\b', r'\bgerman\b',
    r'\bitalian\b', r'\bspanish\b', r'\brussian\b', r'\bindian\b',
    r'\bchinese\b', r'\bjapanese\b', r'\baustralian\b', r'\bcanadian\b',
    r'\bargentine\b', r'\bbrazilian\b', r'\bnigerian\b', r'\bsoviet\b',
]

_REJECT_SIGNALS = [
    r'\bwar\b', r'\bbattle of\b', r'\btreaty\b', r'\boperation\b',
    r'\bcampaign\b', r'\bsiege\b', r'\binvasion\b', r'\brebellion\b',
    r'\brevolt\b', r'\bfilm\b', r'\bmovie\b', r'\balbum\b', r'\bsong\b',
    r'\bship\b', r'\bvessel\b', r'\baircraft\b', r'\bairline\b',
    r'\bcompany\b', r'\bcorporation\b', r'\binc\.?\b', r'\bltd\.?\b',
    r'\borganis[a-z]+\b', r'\borganiz[a-z]+\b',
    r'\bbuilding\b', r'\bbridge\b', r'\btunnel\b', r'\bstadium\b',
    r'\bnewspaper\b', r'\bmagazine\b', r'\bjournal\b',
    r'\bearthquake\b', r'\bhurricane\b', r'\bcyclone\b', r'\bflood\b',
    r'\bmassacre\b', r'\bbombing\b',
    r'\buniversity\b', r'\bcollege\b', r'\bschool\b', r'\blibrary\b',
]


def _is_person(title: str, description: str) -> bool:
    combined = (description + " " + title).lower()
    for pat in _REJECT_SIGNALS:
        if re.search(pat, combined):
            return False
    for pat in _PERSON_SIGNALS:
        if re.search(pat, combined):
            return True
    parts = title.split()
    if 2 <= len(parts) <= 4 and all(p[0].isupper() for p in parts if p):
        stopwords = {"of", "the", "and", "in", "at", "by", "for", "to", "on"}
        if not any(p.lower() in stopwords for p in parts):
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Fetch biography and extract years from it
# ─────────────────────────────────────────────────────────────────────────────

WP_API = "https://en.wikipedia.org/w/api.php"


def get_biography(title: str) -> str:
    params = urllib.parse.urlencode({
        "action":          "query",
        "titles":          title,
        "prop":            "extracts",
        "explaintext":     "1",
        "exsectionformat": "plain",
        "format":          "json",
        "formatversion":   "2",
    })
    data = http_get_json(f"{WP_API}?{params}")
    pages = data.get("query", {}).get("pages", [])
    if not pages or pages[0].get("missing"):
        return ""
    return pages[0].get("extract", "")


def extract_years_from_bio(extract: str, category: str, api_year) -> tuple:
    """
    Robust year extraction (v9.3):
    - Strips IPA guides and square-bracketed annotations
    - Finds ALL (YYYY–YYYY) parentheticals in the opening text
    - Picks the one with the earliest first year (= actual birth year)
      which avoids grabbing art-period ranges like (1940–1970) for Rothko
    - Falls back to complex date ranges, then born/died text patterns
    - v9.3: Extended to handle ancient figures (pre-1000 AD) such as Constantine
      whose birth/death years are 1–3 digit numbers
    """
    birth_year = "?"
    death_year = "present"

    head = extract[:600]

    # Strip IPA guide at start of parenthetical: (/ˈrɒθkoʊ/; → (
    head = re.sub(r'\(/[^/)]+/[,;]?\s*', '(', head)
    # Strip square-bracketed content: [O.S. 3 April], [Russian: Маркус...]
    head = re.sub(r'\[[^\[\]]{0,200}\]', '', head)

    # Year pattern: covers ancient (1-999 AD) through modern (1000-2099)
    # Ancient years are matched only when 2-4 digits (avoid matching stray numbers)
    _YR = r'(?:1[0-9]{3}|20[0-9]{2}|[1-9]\d{1,2}|\d{3,4})'

    # 1. Collect ALL year-range parentheticals (both simple and complex forms)
    #    then pick the one with the earliest first year (= true birth year).
    #    This avoids grabbing art-period ranges like (1940–1970) for Rothko
    #    when the real birth–death range is (Sep 25, 1903 – Feb 25, 1970).
    candidates_yr = []

    # Simple form: (YYYY–YYYY)  — includes ancient e.g. (272–337)
    for m in re.finditer(
        r'\(\s*(\b' + _YR + r'\b)\s*[–\-]\s*(\b' + _YR + r'\b)\s*\)',
        head
    ):
        try:
            candidates_yr.append((int(m.group(1)), m.group(1), m.group(2)))
        except ValueError:
            pass

    # Complex form: (DATE YYYY – DATE YYYY)  — includes ancient dates
    for m in re.finditer(
        r'\(\s*[^()]{0,100}?(\b' + _YR + r'\b)[^()]{0,60}?[–\-]\s*[^()]{0,60}?(\b' + _YR + r'\b)\s*\)',
        head
    ):
        try:
            candidates_yr.append((int(m.group(1)), m.group(1), m.group(2)))
        except ValueError:
            pass

    if candidates_yr:
        # Pick the candidate with the earliest first year (= birth year)
        best = min(candidates_yr, key=lambda t: t[0])
        birth_year = best[1]
        death_year = best[2]
        return birth_year, death_year

    # 3. Use API year for the known event
    if category == "births" and api_year:
        birth_year = str(api_year)
    elif category == "deaths" and api_year:
        death_year = str(api_year)

    # 4. Text pattern search with extended windows — includes ancient years
    born_m = re.search(
        r'\bborn\b[^.]{0,120}?\b(' + _YR + r')\b',
        extract[:1500], re.IGNORECASE
    )
    if born_m and birth_year == "?":
        birth_year = born_m.group(1)

    died_m = re.search(
        r'\bdied\b[^.]{0,80}?\b(' + _YR + r')\b',
        extract[:2000], re.IGNORECASE
    )
    if died_m:
        death_year = died_m.group(1)

    # 5. v9.3: If death_year is still "present" but birth_year looks ancient
    #    (< 1000), scan for a 3-4 digit number near "death" or "died" keywords
    if death_year == "present" and birth_year != "?" and birth_year.isdigit() and int(birth_year) < 1000:
        ancient_m = re.search(
            r'\b(?:died?|death|executed?|murdered?|killed)\b[^.]{0,120}?\b([1-9]\d{1,3})\b',
            extract[:3000], re.IGNORECASE
        )
        if ancient_m:
            candidate = int(ancient_m.group(1))
            # Sanity check: death year must be >= birth year
            if candidate >= int(birth_year):
                death_year = ancient_m.group(1)

    return birth_year, death_year


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Score biography
# ─────────────────────────────────────────────────────────────────────────────

_PRIMARY_SIGNALS = {
    "watercooler": [
        r'\brefused\b', r'\binsisted\b', r'\bbetting\b', r'\bwager\b',
        r'\bleg(?:end|endary)\b', r'\brunning joke\b', r'\bquirk\b',
        r'\binfamous\b', r'\bnotorious\b', r'\bsurpris\w+\b',
        r'\bcurious\b', r'\bstrange\b', r'\bremarkabl\w+\b',
        r'\bincident\b', r'\bcharacter\b', r'\bhumou?r\b', r'\bwit\b',
        r'\bwhimsical\b', r'\bbizarre\b', r'\beccentric\b', r'\bpeculiar\b',
        r'\bself-deprecat\w+\b', r'\bfamous for\b', r'\bknown for\b',
        r'\bstory goes\b', r'\baccording to\b', r'\bonce\b',
    ],
    "mental_model": [
        r'\beccentric\b', r'\bhobb(?:y|ies)\b', r'\bphilosophy\b',
        r'\bbelief\b', r'\binvention\b', r'\bpatent\b', r'\btheory\b',
        r'\bdevised\b', r'\bpioneered\b', r'\bunique\b', r'\bunusual\b',
        r'\bunorthodox\b', r'\bself-taught\b', r'\bautodidact\b',
        r'\blast wish\b', r'\bfinal wish\b', r'\bwanted to be\b',
        r'\bbelieved that\b', r'\bfirmly\b', r'\bcreated\b',
        r'\bbuilt\b', r'\bdeveloped\b',
    ],
}

_SECONDARY_SIGNALS = {
    "last_of_kind": [
        r'\blast\b', r'\bfinal\b', r'\bend of an era\b', r'\bextinct\b',
        r'\bvanish\w+\b', r'\bdisappear\w+\b', r'\bobsolete\b',
        r'\bforgotten\b', r'\bnow lost\b',
    ],
    "underdog": [
        r'\bobscur\w+\b', r'\boverlooked\b', r'\bignored\b',
        r'\bunrecogni[sz]\w+\b', r'\bposthum\w+\b', r'\bonly after\b',
        r'\byears later\b', r'\bdespite\b', r'\bstruggl\w+\b',
        r'\bunsung\b', r'\bnever recognised\b',
    ],
    "diy": [
        r'\bself-taught\b', r'\bdropout\b', r'\baccidental\w*\b',
        r'\bby chance\b', r'\bgarage\b', r'\bno formal\b',
        r'\bwithout training\b', r'\bhumble origin\b',
        r'\bno experience\b', r'\bnever.*before\b',
        r'\btaught himself\b', r'\btaught herself\b',
    ],
    "defiance": [
        r'\brefused\b', r'\bdefied\b', r'\bresist\w+\b',
        r'\bfirst woman\b', r'\bfirst black\b', r'\bfirst african\b',
        r'\bfirst person\b', r'\bpersist\w+\b', r'\bcourage\b',
        r'\bbroke\b', r'\bbarrier\b', r'\btrailblaz\w+\b',
        r'\bpioneer\w*\b', r'\bpaved the way\b',
    ],
    "heretic": [
        r'\bdismiss\w+\b', r'\bscoff\w+\b', r'\bvindicat\w+\b',
        r'\bproved.*wrong\b', r'\bskeptic\w*\b', r'\bcontroversi\w+\b',
        r'\bunconventional\b', r'\bmocked\b', r'\bridiculed\b',
        r'\bno one believed\b', r'\bcalled.*crazy\b', r'\bcalled.*mad\b',
        r'\bblasphemy\b', r'\bheresy\b',
    ],
    "humanising": [
        r'\bfeared\b', r'\bcried\b', r'\blaughed\b', r'\bfamily\b',
        r'\bfriend\w*\b', r'\bhumble\b', r'\bmodest\b', r'\bquiet\w*\b',
        r'\bshy\b', r'\bloved\b', r'\bdevoted\b',
        r'\bnever told\b', r'\bkept quiet\b', r'\bkept secret\b',
    ],
    "irony": [
        r'\bironical?ly?\b', r'\bparadox\b', r'\bunexpected\b',
        r'\btwist\b', r'\bdespite\b', r'\bnevertheless\b',
        r'\bcuriously\b', r'\bof all people\b',
        r'\bturned out\b', r'\bwho knew\b', r'\blittle did\b',
        r'\bfailed.*became\b', r'\baccident.*led\b',
    ],
    # --- NEW signal categories informed by user's editorial taste ---
    "direct_quote": [
        r'"[^"]{15,}"', r'\u201c[^\u201d]{15,}\u201d',
        r'\bhe (?:said|told|recalled|wrote)\b',
        r'\bshe (?:said|told|recalled|wrote)\b',
        r'\b(?:he|she) later (?:said|told|recalled|wrote)\b',
        r'\b(?:he|she) once (?:said|told)\b',
        r'\bas (?:he|she) put it\b',
        r'\bin (?:an|a) interview\b',
    ],
    "chance_encounter": [
        r'\boverheard\b', r'\bhappened to\b', r'\bby chance\b',
        r'\bchance (?:meeting|encounter)\b', r'\bwrong turn\b',
        r'\bone day\b', r'\bstumbl\w+\b', r'\baccident\w*\b',
        r'\bsaw an ad\b', r'\bfortuit\w+\b', r'\bserendipit\w+\b',
        r'\bcoincidence\b', r'\bstroke of luck\b',
    ],
    "late_bloom": [
        r'\btoiled in obscurity\b', r'\bdiscovered in (?:her|his)\b',
        r'\blong.delayed\b', r'\bafter retir\w+\b', r'\blate start\b',
        r'\bfinally\b', r'\bin (?:her|his) [5-9]0s\b',
        r'\bin (?:her|his) 80s\b', r'\bfor decades?\b',
        r'\bafter (?:\d+ )?years\b', r'\bwaited\b',
        r'\bnever.*recogni[sz]\w+\b', r'\blong.*before\b',
    ],
    "origin_story": [
        r'\bgrew up\b', r'\bhumble\b', r'\bgarage\b', r'\bfactory\b',
        r'\bbasement\b', r'\bworkshop\b', r'\bfirst job\b',
        r'\bchildhood\b', r'\bfather.*told\b', r'\bmother.*told\b',
        r'\bfather.*wanted\b', r'\bmother.*wanted\b',
        r'\bbefore.*famous\b', r'\bbefore.*career\b',
        r'\bworking.class\b', r'\bpoverty\b', r'\bghetto\b',
    ],
    "vivid_detail": [
        r'\bevery day\b', r'\balways carried\b', r'\bnever miss\w+\b',
        r'\britual\b', r'\bcoffin\b', r'\bnude\b', r'\bhid\w*\b',
        r'\bdisguis\w+\b', r'\bsmuggl\w+\b', r'\bpocket\w*\b',
        r'\bcollect\w+\b', r'\bobsess\w+\b', r'\bmeticulou\w+\b',
        r'\bsecret\w*\b', r'\bhiding\b',
    ],
}

_PRIMARY_THRESHOLD   = 4
_SECONDARY_THRESHOLD = 3


def score_biography(extract: str) -> dict:
    if not extract or len(extract) < 400:
        return {"primary": 0, "secondary": 0, "total": 0, "signals": []}

    text_lower = extract.lower()
    primary = secondary = 0
    signals = []

    for criterion, patterns in _PRIMARY_SIGNALS.items():
        if sum(1 for p in patterns if re.search(p, text_lower)) >= _PRIMARY_THRESHOLD:
            primary += 1
            signals.append(criterion)

    for criterion, patterns in _SECONDARY_SIGNALS.items():
        if sum(1 for p in patterns if re.search(p, text_lower)) >= _SECONDARY_THRESHOLD:
            secondary += 1
            signals.append(criterion)

    length_bonus = min(len(extract) // 4000, 3)
    return {
        "primary":   primary,
        "secondary": secondary,
        "total":     primary * 10 + secondary * 2 + length_bonus,
        "signals":   signals,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — Tagline and section-aware anecdote extraction
# ─────────────────────────────────────────────────────────────────────────────

def clean_tagline(api_description: str, extract: str) -> str:
    """
    Returns a clean single-sentence description with no year clutter.
    Uses the Wikipedia API description first (already curated and short).
    Falls back to the first clean sentence of the biography.
    v8: strips year patterns from output so they don't duplicate the header.
    """
    if api_description and len(api_description) > 15:
        desc = api_description.strip()
        desc = desc[0].upper() + desc[1:]
        if not desc.endswith("."):
            desc += "."
    else:
        # Fallback: strip all parentheticals from first paragraph, return first sentence
        first_para = extract.split("\n\n")[0] if "\n\n" in extract else extract[:800]
        cleaned_para = first_para
        for _ in range(6):
            cleaned_para = re.sub(r'\[[^\[\]]*\]', '', cleaned_para)
            cleaned_para = re.sub(r'\([^()]*\)', '', cleaned_para)
        cleaned_para = re.sub(r'\s{2,}', ' ', cleaned_para).strip()
        sentences = re.split(r'(?<=[.!?])\s+', cleaned_para)
        desc = ""
        for sent in sentences[:4]:
            sent = sent.strip()
            if len(sent) > 40:
                desc = sent
                break
        if not desc:
            desc = cleaned_para[:200] if cleaned_para else ""

    # Strip redundant year patterns (they appear in the header already)
    desc = re.sub(r'\(\s*\d{4}\s*[–\-]\s*\d{4}\s*\)', '', desc)
    desc = re.sub(r'\b(?:born|died)\b\s+\d{4}\b', '', desc, flags=re.IGNORECASE)
    # Remove empty parentheticals left behind, e.g. "()" or "( )"
    desc = re.sub(r'\(\s*\)', '', desc)
    # Clean up whitespace artefacts
    desc = re.sub(r'\s+\.', '.', desc)
    desc = re.sub(r'\s{2,}', ' ', desc).strip()
    if desc and not desc.endswith("."):
        desc += "."
    return desc


# ── Section classification ────────────────────────────────────────────────────

_SKIP_SECTION_KEYWORDS = {
    "popular culture", "in fiction", "cultural legacy", "cultural impact",
    "cultural depictions", "adaptations", "film adaptations", "in media",
    "books about", "novels about", "filmography", "discography",
    "bibliography", "works", "publications", "selected works",
    "see also", "references", "notes", "further reading", "external links",
    "awards", "honours", "honors", "decorations", "legacy",
    "political legacy", "historical legacy", "historiography",
    "critical reception", "critical analysis", "assessment",
    "reputation", "influence", "impact", "commemoration",
    "memorials", "statues", "postage stamps",
}

_PREFER_SECTION_KEYWORDS = {
    "personal life", "private life", "early life", "childhood",
    "early years", "youth", "education", "upbringing",
    "character", "personality", "personal beliefs", "religion",
    "family", "marriages", "relationships", "health",
    "later life", "later years", "death", "final years",
    "anecdotes", "personal", "private",
}


def _section_score(header: str) -> float:
    h = header.lower().strip()
    for kw in _SKIP_SECTION_KEYWORDS:
        if kw in h:
            return 0.0
    for kw in _PREFER_SECTION_KEYWORDS:
        if kw in h:
            return 2.0
    return 1.0


def _split_sections(extract: str) -> list:
    """
    Split a Wikipedia plain-text extract into sections.
    Returns a list of dicts: {header, text, multiplier}.
    """
    sections = []
    current_header = ""
    current_chunks = []

    paragraphs = extract.split("\n\n")

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        lines = para.splitlines()
        first_line = lines[0].strip()

        is_header = (
            len(first_line) > 0
            and len(first_line) < 80
            and first_line[-1] not in ".!?,;"
            and not re.match(r'^\d', first_line)
            and len(lines) == 1
        )

        if is_header and current_chunks:
            sections.append({
                "header":     current_header,
                "text":       " ".join(current_chunks),
                "multiplier": _section_score(current_header),
            })
            current_header = first_line
            current_chunks = []
        elif is_header:
            current_header = first_line
        else:
            current_chunks.append(para)

    if current_chunks:
        sections.append({
            "header":     current_header,
            "text":       " ".join(current_chunks),
            "multiplier": _section_score(current_header),
        })

    return sections


_CULTURAL_SENTENCE_PATTERNS = [
    r'\bportray\w*\b.*\bfilm\b',
    r'\bfilm\b.*\bportray\w*\b',
    r'\btelevision (series|film|movie|show)\b',
    r'\bdocumentary\b',
    r'\bnovel\b.*\babout\b',
    r'\bbiopic\b',
    r'\bplayed by\b',
    r'\b(starred|starring)\b',
    r'\bminiseries\b',
    r'\bopera\b.*\bbased on\b',
]


def _is_cultural_sentence(sentence: str) -> bool:
    s = sentence.lower()
    return any(re.search(p, s) for p in _CULTURAL_SENTENCE_PATTERNS)


# v9.3: Words that mark a sentence as a mid-thought continuation fragment.
# Sentences beginning with these words are likely torn from a longer sentence.
_CONTINUATION_STARTS = frozenset({
    'and', 'but', 'or', 'nor', 'yet', 'so', 'for',
    'however', 'although', 'though', 'while', 'whilst',
    'which', 'who', 'whom', 'whose', 'that', 'where', 'when',
    'because', 'since', 'after', 'before', 'until', 'once',
    'his', 'her', 'their', 'its',
    'with', 'through', 'via', 'upon', 'among', 'between',
    'as', 'if', 'unless', 'despite', 'despite',
    'including', 'having', 'being', 'making', 'leaving',
})


def _is_sentence_fragment(s: str) -> bool:
    """
    Return True if the string looks like a mid-thought sentence fragment
    rather than a properly started sentence.
    """
    if not s:
        return True
    # Must start with an uppercase letter
    if not s[0].isupper():
        return True
    # Must not begin with a continuation word
    first_word = re.split(r'[\s,;:]', s)[0].lower().rstrip('.,;:')
    return first_word in _CONTINUATION_STARTS


def extract_anecdote(extract: str, signals: list) -> list:
    """
    v8: Returns a list of {"label": str, "text": str} dicts — 2 to 3 snippets
    from distinct sections, ~500 words total.

    Strategy:
    1. Preferred sections (Personal life, Early life, etc.) are tried first
    2. Normal biographical sections fill remaining budget
    3. Each snippet targets ~170-200 words, starting near the most signal-rich sentence
    4. Cultural legacy / political analysis sections are skipped entirely
    5. If a section yields < 20 words it is discarded (too thin)
    """
    all_sigs = {**_PRIMARY_SIGNALS, **_SECONDARY_SIGNALS}
    scoring_patterns = []
    for sig in signals:
        scoring_patterns.extend(all_sigs.get(sig, []))
    if not scoring_patterns:
        for patterns in _SECONDARY_SIGNALS.values():
            scoring_patterns.extend(patterns)

    sections = _split_sections(extract)

    preferred = [(s["header"], s["text"]) for s in sections if s["multiplier"] == 2.0]
    normal    = [(s["header"], s["text"]) for s in sections if s["multiplier"] == 1.0]

    pool = preferred + normal   # preferred sections come first

    snippets    = []
    total_words = 0
    used_labels: dict = {}   # v9.3: track label usage to prevent duplicates

    for header, text in pool:
        if len(snippets) >= 3 or total_words >= 500:
            break

        sentences = [
            s.strip() for s in re.split(r'(?<=[.!?])\s+', text)
            if len(s.strip()) > 35
            and not _is_cultural_sentence(s)
            and not _is_sentence_fragment(s.strip())   # v9.3: drop fragments
        ]
        if not sentences:
            continue

        # Score each sentence by signal density
        scored = []
        for i, sent in enumerate(sentences):
            hits = sum(1 for p in scoring_patterns if re.search(p, sent.lower()))
            scored.append((hits, i, sent))
        scored.sort(key=lambda x: (-x[0], x[1]))

        best_idx = scored[0][1]

        # Word budget per snippet: ~200 words, but respect remaining budget
        budget = min(200, 500 - total_words)

        # Window starting one sentence before the richest hit
        start  = max(0, best_idx - 1)
        window = []
        wc     = 0
        for i in range(start, len(sentences)):
            sw = len(sentences[i].split())
            if wc + sw > budget and window:
                break
            window.append(sentences[i])
            wc += sw

        if wc < 20:   # Too thin — skip this section
            continue

        # Format the section label for display
        label = header.strip().title() if header.strip() else "Life & Character"
        # Replace bland generic labels
        if label.lower() in {"biography", "life", "overview", "introduction", "background", ""}:
            label = "Life & Character"

        # v9.3: Prevent duplicate labels (e.g. two "Life & Character" sections)
        base_label = label
        count = used_labels.get(base_label, 0) + 1
        used_labels[base_label] = count
        if count > 1:
            suffixes = ["II", "III", "IV", "V"]
            label = f"{base_label} ({suffixes[min(count - 2, len(suffixes) - 1)]})"

        snippets.append({"label": label, "text": " ".join(window)})
        total_words += wc

    if not snippets:
        # Ultimate fallback: take up to 400 words from non-cultural extract text
        all_sents = [
            s.strip() for s in re.split(r'(?<=[.!?])\s+', extract)
            if len(s.strip()) > 35
            and not _is_cultural_sentence(s)
            and not _is_sentence_fragment(s.strip())
        ]
        fallback_text = []
        wc = 0
        for s in all_sents:
            sw = len(s.split())
            if wc + sw > 400:
                break
            fallback_text.append(s)
            wc += sw
        snippets = [{"label": "Life & Character", "text": " ".join(fallback_text)}]

    return snippets


def has_rich_anecdote(candidate: dict) -> bool:
    """
    Returns True if the candidate has substantive personal/human-interest content.
    Used to swap out candidates whose biographies are too dry.
    """
    snippets = candidate.get("anecdote_snippets", [])
    if not snippets:
        return False
    total_words = sum(len(s.get("text", "").split()) for s in snippets)
    # Rich = at least 2 labelled snippets OR 100+ words of meaningful content
    return len(snippets) >= 2 or total_words >= 100


# ─────────────────────────────────────────────────────────────────────────────
# Step 4b — Obituary Digest (v9)
#   Fetches recent obituaries from NYT + Guardian RSS feeds,
#   resolves archive URLs, scores, selects 2 per publication,
#   and generates copyright-safe 2-3 sentence teasers.
# ─────────────────────────────────────────────────────────────────────────────

_OBITUARY_RSS_FEEDS = {
    "NYT": "https://rss.nytimes.com/services/xml/rss/nyt/Obituaries.xml",
    "Guardian": "https://www.theguardian.com/tone/obituaries/rss",
}


def _fetch_rss(url: str) -> list:
    """Fetch and parse an RSS feed. Returns list of item dicts."""
    items = []
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            xml_bytes = resp.read()
        root = ET.fromstring(xml_bytes)

        for item in root.findall(".//item"):
            title    = (item.findtext("title") or "").strip()
            link     = (item.findtext("link") or "").strip()
            desc     = (item.findtext("description") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()

            if not title or not link:
                continue

            # Parse publication date (RFC 822)
            pub_dt = None
            if pub_date:
                try:
                    pub_dt = parsedate_to_datetime(pub_date).date()
                except Exception:
                    pass

            items.append({
                "title":    title,
                "link":     link,
                "desc":     desc,
                "pub_date": pub_dt,
            })
    except Exception as exc:
        log.warning("Failed to fetch RSS %s: %s", url, exc)

    return items


_JS_CONTAMINATION_PATTERNS = [
    # Arrow functions, method calls
    r'=>\s*\{',
    r'\bforEach\s*\(',
    r'\baddEventListener\s*\(',
    r'\bquerySelector\s*\(',
    r'\bgetAttribute\s*\(',
    r'\bsetAttribute\s*\(',
    r'\bclassList\b',
    # Variable declarations
    r'\bconst\s+\w+\s*=',
    r'\blet\s+\w+\s*=',
    r'\bvar\s+\w+\s*=',
    # Common JS constructs
    r'\bfunction\s*\(',
    r'\btabindex\b',
    r'\bisOpen\b',
    r'\bnull\b\s*:\s*\b',   # ternary null : value
    # Guardian-specific menu JS
    r'expandedMenu',
    r'veggie-burger',
    r'Clickable\w*Tags',
]


def _is_js_contaminated(sentence: str) -> bool:
    """Return True if the sentence looks like leaked JavaScript code."""
    return any(re.search(p, sentence) for p in _JS_CONTAMINATION_PATTERNS)


def _clean_html_chunk(chunk: str) -> str:
    """Strip tags and entities from a single HTML chunk, return plain text."""
    text = re.sub(r'<[^>]+>', ' ', chunk)
    text = (text
            .replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
            .replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' ')
            .replace('&#x27;', "'").replace('&#x2F;', '/'))
    text = re.sub(r'&\w+;', '', text)
    return re.sub(r'\s+', ' ', text).strip()


def _strip_html_tags(html: str) -> str:
    """
    HTML→text conversion for article extraction (v9.4).

    - Strips script/style/noscript blocks
    - Extracts <p> tag content, preserving paragraph breaks as \\n\\n
      so downstream paragraph-aware teaser extraction can work with them
    - Filters sentences that look like leaked JavaScript code
    - Falls back to flat-text extraction when no <p> tags found
    """
    # Remove entire script/style/noscript blocks first
    clean = re.sub(r'<(script|style|noscript)[^>]*>.*?</\1>', '', html,
                   flags=re.DOTALL | re.IGNORECASE)

    paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', clean, flags=re.DOTALL)
    if paragraphs:
        # Process each paragraph individually to preserve structure
        clean_paras = []
        for raw_para in paragraphs:
            para_text = _clean_html_chunk(raw_para)
            if not para_text:
                continue
            # Filter JS-contaminated sentences within this paragraph
            sents = re.split(r'(?<=[.!?])\s+', para_text)
            good_sents = [s for s in sents if not _is_js_contaminated(s)]
            clean_para = ' '.join(good_sents).strip()
            if clean_para and len(clean_para) > 20:
                clean_paras.append(clean_para)
        return '\n\n'.join(clean_paras)

    # Fallback: no <p> tags — process as flat text
    text = _clean_html_chunk(clean)
    parts = re.split(r'(?<=[.!?])\s+', text)
    return ' '.join(p for p in parts if not _is_js_contaminated(p)).strip()


def _extract_og_description(html_head: str) -> str:
    """
    Extract the og:description meta tag value from page HTML.
    Works even on paywalled pages since the <head> is always served in full.
    The og:description is typically the editorial lede (1-3 rich sentences).
    """
    # Two common attribute orderings
    for pattern in (
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']{20,})["\']',
        r'<meta[^>]+content=["\']([^"\']{20,})["\'][^>]+property=["\']og:description["\']',
        # Also handle name="description" as a fallback
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']{20,})["\']',
        r'<meta[^>]+content=["\']([^"\']{20,})["\'][^>]+name=["\']description["\']',
    ):
        m = re.search(pattern, html_head, re.IGNORECASE)
        if m:
            desc = m.group(1).strip()
            desc = (desc
                    .replace('&amp;', '&').replace('&#39;', "'")
                    .replace('&quot;', '"').replace('&lt;', '<')
                    .replace('&gt;', '>').replace('&#x27;', "'")
                    .replace('&#x2F;', '/'))
            return desc
    return ""


def _resolve_archive_url(original_url: str) -> tuple:
    """
    Try archive.ph → Wayback Machine → original URL → og:description.
    Returns (best_url, article_text).

    v9.3: When article_text < 500 chars (paywalled NYT, etc.), fetch the
    original URL's <head> and extract the og:description meta tag, which
    editors write as a rich lede and is served even behind a paywall.
    """
    best_url = original_url
    article_text = ""

    # 1. archive.ph
    try:
        archive_url = f"https://archive.ph/newest/{original_url}"
        req = urllib.request.Request(archive_url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            final_url = resp.url
            if "archive.ph/" in final_url:
                html = resp.read().decode("utf-8", errors="replace")
                best_url = final_url
                article_text = _strip_html_tags(html)
    except Exception:
        pass

    if len(article_text) >= 500:
        return best_url, article_text

    # 2. Wayback Machine availability API
    try:
        wb_api = (
            "https://archive.org/wayback/available?url="
            + urllib.parse.quote(original_url, safe="")
        )
        data = http_get_json(wb_api)
        closest = data.get("archived_snapshots", {}).get("closest", {})
        if closest.get("available"):
            wb_url = closest["url"]
            req = urllib.request.Request(wb_url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=20) as resp:
                html = resp.read().decode("utf-8", errors="replace")
                wb_text = _strip_html_tags(html)
                if len(wb_text) > len(article_text):
                    best_url = wb_url
                    article_text = wb_text
    except Exception:
        pass

    if len(article_text) >= 500:
        return best_url, article_text

    # 3. Original URL (Guardian often not paywalled)
    try:
        req = urllib.request.Request(original_url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
            direct_text = _strip_html_tags(html)
            if len(direct_text) > len(article_text):
                best_url = original_url
                article_text = direct_text
    except Exception:
        pass

    if len(article_text) >= 500:
        return best_url, article_text

    # 4. v9.3: og:description from original URL <head> (works even behind paywall)
    #    Read only the first 15 KB — enough to capture <head> without the article body
    try:
        req = urllib.request.Request(original_url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=15) as resp:
            partial_html = resp.read(15000).decode("utf-8", errors="replace")
        og_desc = _extract_og_description(partial_html)
        if og_desc:
            log.info("og:description fallback used for %s (%d chars)", original_url, len(og_desc))
            # Prepend og:description to whatever thin text we already have
            article_text = og_desc + (" " + article_text if article_text else "")
            best_url = original_url
    except Exception:
        pass

    return best_url, article_text


def _score_obituary_text(text: str) -> dict:
    """Score obituary text using the same editorial signal patterns."""
    if not text or len(text) < 200:
        return {"primary": 0, "secondary": 0, "total": 0, "signals": []}

    text_lower = text.lower()
    primary = secondary = 0
    signals = []

    for criterion, patterns in _PRIMARY_SIGNALS.items():
        hits = sum(1 for p in patterns if re.search(p, text_lower))
        if hits >= 3:   # slightly lower threshold for shorter texts
            primary += 1
            signals.append(criterion)

    for criterion, patterns in _SECONDARY_SIGNALS.items():
        hits = sum(1 for p in patterns if re.search(p, text_lower))
        if hits >= 2:
            secondary += 1
            signals.append(criterion)

    return {
        "primary":   primary,
        "secondary": secondary,
        "total":     primary * 10 + secondary * 2 + random.uniform(0, 2),
        "signals":   signals,
    }


def _score_text_block(block: str, patterns: list, position: int, total: int) -> float:
    """
    Score a text block (sentence or paragraph) for editorial interest.
    Shared by both the paragraph-level and sentence-level teaser paths.
    """
    block_lower = block.lower()
    hits = float(sum(1 for p in patterns if re.search(p, block_lower)))

    # Direct quote bonus — user's highlights overwhelmingly feature quoted speech
    has_quote = bool(re.search(r'["\u201c\u2018][^"\u201d\u2019]{15,}["\u201d\u2019]', block))
    has_speech = bool(re.search(
        r'\b(?:he|she|they)\s+(?:said|told|recalled|wrote|added|continued|explained|noted|remembered)\b',
        block, re.IGNORECASE
    ))
    quote_bonus = (2.5 if has_quote else 0.0) + (1.0 if has_speech else 0.0)

    # Vivid detail and origin story bonuses
    vivid_pats  = _SECONDARY_SIGNALS.get("vivid_detail", [])
    origin_pats = _SECONDARY_SIGNALS.get("origin_story", [])
    vivid_bonus  = min(sum(1 for p in vivid_pats  if re.search(p, block_lower)) * 0.5, 1.5)
    origin_bonus = min(sum(1 for p in origin_pats if re.search(p, block_lower)) * 0.3, 0.9)

    # Lede preference: first 30% of blocks carry narrative context
    pos_bonus = 0.4 if position < max(total * 0.30, 1) else 0.0

    return hits + quote_bonus + vivid_bonus + origin_bonus + pos_bonus


def _extract_teaser(text: str, signals: list) -> str:
    """
    v9.4: Paragraph-first teaser extraction.

    When the article text has ≥3 proper paragraphs (preserved from HTML <p>
    tags by _strip_html_tags()), this picks the 3 highest-scoring paragraphs
    and returns them joined by '\\n\\n' so the obituary card can render each
    as its own <p> block — giving the flowing narrative the user wants
    (e.g. the Jean Wilson-style multi-paragraph story with quotes and context).

    Falls back to sentence-level extraction for flat text (og:description,
    RSS descriptions) that has no paragraph structure.
    """
    if not text:
        return ""

    all_sigs = {**_PRIMARY_SIGNALS, **_SECONDARY_SIGNALS}
    patterns: list = []
    for sig in signals:
        patterns.extend(all_sigs.get(sig, []))
    if not patterns:
        for pats in _SECONDARY_SIGNALS.values():
            patterns.extend(pats)

    # ── Paragraph-level path ─────────────────────────────────────────────────
    raw_paras = [p.strip() for p in re.split(r'\n\n+', text) if len(p.strip()) > 80]

    if len(raw_paras) >= 3:
        scored = [
            (_score_text_block(p, patterns, i, len(raw_paras)), i, p)
            for i, p in enumerate(raw_paras)
        ]
        scored.sort(key=lambda x: (-x[0], x[1]))
        # Pick top 3, restore original order so they read as a narrative
        top = sorted(scored[:3], key=lambda x: x[1])
        return '\n\n'.join(t[2] for t in top)

    # ── Sentence-level fallback (flat text: og:description, RSS, etc.) ───────
    sentences = [
        s.strip() for s in re.split(r'(?<=[.!?])\s+', text)
        if 40 < len(s.strip()) < 400
    ]
    if not sentences:
        return text[:600].strip()

    scored_s = [
        (_score_text_block(s, patterns, i, len(sentences)), i, s)
        for i, s in enumerate(sentences)
    ]
    scored_s.sort(key=lambda x: (-x[0], x[1]))
    top_indices = sorted(s[1] for s in scored_s[:6])
    return " ".join(sentences[i] for i in top_indices)


def _extract_obit_years(title: str, desc: str, text: str) -> tuple:
    """Extract birth and death years from obituary title/text."""
    combined = f"{title} {desc} {text[:500]}"

    # Pattern: "Name, 85, Dies" or "who died aged 85"
    age_m = re.search(r'\b(\d{2,3})\b[,.]?\s*(?:dies|dead|has died|who died)',
                      combined, re.IGNORECASE)

    # Direct year patterns
    death_m = re.search(r'\b(202[0-9])\b', combined)
    birth_m = re.search(
        r'(?:born|b\.)\s*(?:in\s+)?(\b(?:19|20)\d{2}\b)',
        combined, re.IGNORECASE
    )

    death_year = death_m.group(1) if death_m else "2026"
    birth_year = birth_m.group(1) if birth_m else "?"

    # If we have age + death year but no birth year, compute it
    if birth_year == "?" and age_m:
        try:
            age = int(age_m.group(1))
            dy  = int(death_year)
            birth_year = str(dy - age)
        except ValueError:
            pass

    return birth_year, death_year


def _extract_obit_tagline(title: str, desc: str) -> str:
    """Build a clean one-sentence tagline from the RSS title/description."""
    # Guardian titles often include "Obituary" suffix — strip it
    tag = desc if len(desc) > 30 else title
    tag = re.sub(r'\s*[–\-|]\s*obituar\w*\s*$', '', tag, flags=re.IGNORECASE)
    tag = re.sub(r'\s*obituar\w*:?\s*', '', tag, flags=re.IGNORECASE)
    tag = re.sub(r'\s+', ' ', tag).strip()
    # Strip HTML tags from RSS description
    tag = re.sub(r'<[^>]+>', '', tag).strip()
    # Limit to first sentence
    sentences = re.split(r'(?<=[.!?])\s+', tag)
    tag = sentences[0] if sentences else tag
    if tag and not tag.endswith("."):
        tag += "."
    return tag[:250]


def fetch_obituaries() -> list:
    """
    Fetch recent obituaries from NYT and Guardian RSS feeds.
    Returns list of scored obituary candidates.
    """
    today = datetime.date.today()
    cutoff = today - datetime.timedelta(days=7)

    all_obits = []

    for source, rss_url in _OBITUARY_RSS_FEEDS.items():
        log.info("Fetching %s obituary RSS…", source)
        items = _fetch_rss(rss_url)
        log.info("%s RSS: %d items", source, len(items))

        # Filter to past 7 days
        recent = [
            it for it in items
            if it["pub_date"] is None or it["pub_date"] >= cutoff
        ]
        log.info("%s recent (past 7 days): %d", source, len(recent))

        # Quick-score based on RSS description to pre-filter
        for item in recent:
            item["source"]  = source
            item["_quick"]  = _score_obituary_text(item["desc"])
        recent.sort(key=lambda x: -x["_quick"]["total"])

        # Take top 8 per source for deeper processing
        top_n = recent[:8]

        for item in top_n:
            log.info("Resolving archive for: %s", item["title"])
            archive_url, article_text = _resolve_archive_url(item["link"])

            # v9.4: For NYT, the link in the email must always point to archive.ph
            # so the reader can open the full article (bypassing the paywall).
            # If _resolve_archive_url() didn't land on an archive.ph URL (e.g. it
            # fell back to og:description), we force the link URL to archive.ph/newest/…
            if source == "NYT":
                if "archive.ph/" not in archive_url:
                    archive_url = f"https://archive.ph/newest/{item['link']}"
                    log.info("NYT link overridden to archive.ph: %s", archive_url)

            item["archive_url"]  = archive_url
            item["article_text"] = article_text

            # Full-text scoring (falls back to description if fetch failed)
            scoring_text = article_text if len(article_text) > 200 else item["desc"]
            item["score"]   = _score_obituary_text(scoring_text)
            item["signals"] = item["score"]["signals"]

            # Extract teaser and metadata
            item["teaser"]  = _extract_teaser(
                article_text if article_text else item["desc"],
                item["signals"]
            )
            birth_yr, death_yr = _extract_obit_years(
                item["title"], item["desc"], article_text
            )
            item["birth_year"] = birth_yr
            item["death_year"] = death_yr
            item["tagline"]    = _extract_obit_tagline(item["title"], item["desc"])

            # Extract person name from title (strip "dies at 85" etc.)
            name = re.sub(
                r'\s*[,:].*(?:dies?|dead|obituary|has died).*$', '',
                item["title"], flags=re.IGNORECASE
            ).strip()
            name = re.sub(r'\s*obituary\s*$', '', name, flags=re.IGNORECASE).strip()
            item["name"] = name if name else item["title"]

            all_obits.append(item)
            time.sleep(1.0)  # Be polite to archive services

    return all_obits


def select_obituaries(candidates: list) -> list:
    """Select 2 from NYT and 2 from Guardian, best-scored."""
    nyt = sorted(
        [c for c in candidates if c["source"] == "NYT"],
        key=lambda x: -x["score"]["total"]
    )
    guardian = sorted(
        [c for c in candidates if c["source"] == "Guardian"],
        key=lambda x: -x["score"]["total"]
    )
    selected = nyt[:2] + guardian[:2]
    log.info("Selected obituaries: %s",
             [(o["name"], o["source"]) for o in selected])
    return selected


_OBIT_TAG = (
    "display:inline-block;background:#fdf2e9;color:#8b5e3c;"
    "border-radius:5px;font-size:11px;font-weight:600;"
    "padding:3px 9px;margin:2px 3px 2px 0;letter-spacing:.04em;"
)


def _obituary_card(o: dict) -> str:
    """Generate a single obituary HTML card."""
    birth = o.get("birth_year", "?")
    death = o.get("death_year", "?")
    years = f"({birth}–{death})" if birth != "?" else ""
    source_label = "The New York Times" if o["source"] == "NYT" else "The Guardian"

    tags = "".join(
        f'<span style="{_OBIT_TAG}">{SIGNAL_LABELS.get(s, s)}</span>'
        for s in o.get("signals", [])[:3]
    )
    tags_block = f'<div style="margin-top:13px;">{tags}</div>' if tags else ""

    # v9.4: render multi-paragraph teasers as separate <p> blocks so the
    # narrative flows like a proper editorial piece rather than a run-on string.
    raw_teaser = o.get("teaser", "")
    _para_style = (
        'style="font-size:15px;line-height:1.78;color:#2d2d2d;'
        'margin:0 0 14px 0;"'
    )
    _para_last_style = (
        'style="font-size:15px;line-height:1.78;color:#2d2d2d;margin:0;"'
    )
    teaser_paras = [p.strip() for p in re.split(r'\n\n+', raw_teaser) if p.strip()]
    if len(teaser_paras) > 1:
        teaser_html = "".join(
            f'<p {_para_style}>{p}</p>' if idx < len(teaser_paras) - 1
            else f'<p {_para_last_style}>{p}</p>'
            for idx, p in enumerate(teaser_paras)
        )
    else:
        teaser_html = f'<p {_para_last_style}>{raw_teaser}</p>'

    return f"""
    <div style="background:#ffffff;border:1px solid #e5ddd4;border-radius:12px;
                padding:24px 26px 20px;margin-bottom:24px;">
      <p style="font-size:11px;font-weight:600;color:#8b5e3c;text-transform:uppercase;
                letter-spacing:.09em;margin:0 0 7px;">{source_label}</p>
      <div style="margin-bottom:4px;">
        <span style="font-size:20px;font-weight:700;color:#1a1a1a;">{o['name']}</span>
        <span style="font-size:13px;color:#6b7280;margin-left:8px;">{years}</span>
        <p style="font-size:14px;color:#555;font-style:italic;margin:6px 0 0;">{o['tagline']}</p>
      </div>
      <p style="font-size:11px;font-weight:700;color:#8b5e3c;letter-spacing:.1em;
                text-transform:uppercase;margin:16px 0 12px;">The Anecdote</p>
      {teaser_html}
      {tags_block}
      <a href="{o['archive_url']}"
         style="display:inline-block;margin-top:18px;font-size:14px;
                color:#2563eb;text-decoration:none;font-weight:500;">
        Read the full obituary &rarr;
      </a>
    </div>"""


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — Select 4 with era AND field/nationality diversity
# ─────────────────────────────────────────────────────────────────────────────

def _diversity_key(candidate: dict) -> str:
    desc = candidate.get("description", "").lower()

    nationalities = [
        "american", "british", "english", "scottish", "irish", "welsh",
        "french", "german", "italian", "spanish", "russian", "soviet",
        "chinese", "japanese", "indian", "australian", "canadian",
        "argentine", "brazilian", "nigerian", "south african", "egyptian",
        "polish", "dutch", "swedish", "norwegian", "greek", "turkish",
        "mexican", "cuban", "venezuelan", "colombian", "chilean",
    ]
    nationality = next((n for n in nationalities if n in desc), "other")

    field_map = {
        "politics":   ["politician", "president", "prime minister", "senator",
                       "minister", "statesman", "diplomat", "governor", "chancellor"],
        "military":   ["general", "admiral", "commander", "colonel", "marshal"],
        "science":    ["scientist", "physicist", "chemist", "biologist",
                       "mathematician", "astronomer", "geologist", "inventor", "engineer"],
        "arts":       ["painter", "sculptor", "architect", "artist", "photographer"],
        "music":      ["composer", "musician", "singer", "pianist", "conductor"],
        "literature": ["author", "writer", "poet", "novelist", "playwright"],
        "royalty":    ["king", "queen", "emperor", "empress", "prince", "princess",
                       "monarch", "pharaoh", "tsar", "tsarina"],
        "sport":      ["athlete", "footballer", "boxer", "cricketer", "tennis",
                       "cyclist", "swimmer", "jockey"],
        "film_tv":    ["actor", "actress", "director", "filmmaker", "producer"],
        "religion":   ["archbishop", "bishop", "theologian", "pope", "cardinal"],
        "activism":   ["activist", "reformer", "revolutionary", "dissident"],
    }
    field = "other"
    for f, keywords in field_map.items():
        if any(kw in desc for kw in keywords):
            field = f
            break

    return f"{nationality}_{field}"


def _era(birth_year: str) -> str:
    try:
        y = int(birth_year)
        if y < 1700:  return "pre-1700"
        if y < 1850:  return "1700-1849"
        if y < 1940:  return "1850-1939"
        return "modern"
    except ValueError:
        return "unknown"


def select_four(ranked: list) -> list:
    """
    Select 4 from the ranked list, enforcing:
    - No more than 2 from the same era
    - No more than 1 from the same nationality+field combination
    """
    if len(ranked) <= 4:
        return ranked

    selected     = []
    era_counts   = {}
    div_key_used = set()

    for p in ranked:
        era = _era(p["birth_year"])
        dk  = _diversity_key(p)

        if era_counts.get(era, 0) >= 2 and len(ranked) > 6:
            continue
        if dk in div_key_used and len(ranked) > 6:
            continue

        selected.append(p)
        era_counts[era] = era_counts.get(era, 0) + 1
        div_key_used.add(dk)

        if len(selected) == 4:
            break

    # Fill remaining slots if diversity rules left us short
    if len(selected) < 4:
        for p in ranked:
            if p not in selected:
                selected.append(p)
            if len(selected) == 4:
                break

    return selected[:4]


# ─────────────────────────────────────────────────────────────────────────────
# Step 6 — Build and send HTML email
# ─────────────────────────────────────────────────────────────────────────────

SIGNAL_LABELS = {
    "watercooler":       "Watercooler Anecdote",
    "mental_model":      "Eccentric Mental Model",
    "last_of_kind":      "Last of a Kind",
    "underdog":          "Underdog / Overlooked",
    "diy":               "Hidden Origin / DIY",
    "defiance":          "Strategic Defiance",
    "heretic":           "Scientific Heretic",
    "humanising":        "Humanising Contrast",
    "irony":             "Narrative Irony",
    # New labels (v9.1)
    "direct_quote":      "Voice of the Person",
    "chance_encounter":  "Pivotal Chance Moment",
    "late_bloom":        "Late Discovery",
    "origin_story":      "Origin Story",
    "vivid_detail":      "Vivid Personal Detail",
}

_TAG = (
    "display:inline-block;background:#eef7f2;color:#2e6e4e;"
    "border-radius:5px;font-size:11px;font-weight:600;"
    "padding:3px 9px;margin:2px 3px 2px 0;letter-spacing:.04em;"
)

_SNIPPET_LABEL_STYLE = (
    "font-size:11px;font-weight:700;color:#9ca3af;"
    "letter-spacing:.08em;text-transform:uppercase;"
    "margin:14px 0 3px;"
)

_SNIPPET_TEXT_STYLE = (
    "font-size:15px;line-height:1.78;color:#2d2d2d;margin:0 0 6px;"
)


def _card(p: dict) -> str:
    birth  = p["birth_year"]
    death  = p["death_year"]
    years  = f"({birth}–{death})" if birth != "?" else ""
    source = "Born on this date" if p["source"] == "births" else "Died on this date"

    tags = "".join(
        f'<span style="{_TAG}">{SIGNAL_LABELS.get(s, s)}</span>'
        for s in p.get("signals", [])[:4]
    )
    tags_block = f'<div style="margin-top:13px;">{tags}</div>' if tags else ""

    # Render labelled snippets
    snippets = p.get("anecdote_snippets", [])
    if snippets:
        parts = []
        for i, snippet in enumerate(snippets):
            label = snippet.get("label", "")
            text  = snippet.get("text", "")
            top_margin = "margin:16px 0 3px;" if i == 0 else "margin:14px 0 3px;"
            label_style = _SNIPPET_LABEL_STYLE.replace("margin:14px 0 3px;", top_margin)
            if label:
                parts.append(f'<p style="{label_style}">{label}</p>')
            parts.append(f'<p style="{_SNIPPET_TEXT_STYLE}">{text}</p>')
        anecdote_block = "\n      ".join(parts)
    else:
        # Should rarely reach here — fallback for safety
        anecdote_block = (
            f'<p style="{_SNIPPET_TEXT_STYLE}">{p.get("anecdote", "")}</p>'
        )

    return f"""
    <div style="background:#ffffff;border:1px solid #e5e0d8;border-radius:12px;
                padding:24px 26px 20px;margin-bottom:24px;">
      <p style="font-size:11px;font-weight:600;color:#9ca3af;text-transform:uppercase;
                letter-spacing:.09em;margin:0 0 7px;">{source}</p>
      <div style="margin-bottom:4px;">
        <span style="font-size:20px;font-weight:700;color:#1a1a1a;">{p['name']}</span>
        <span style="font-size:13px;color:#6b7280;margin-left:8px;">{years}</span>
        <p style="font-size:14px;color:#555;font-style:italic;margin:6px 0 0;">{p['tagline']}</p>
      </div>
      <p style="font-size:11px;font-weight:700;color:#2e6e4e;letter-spacing:.1em;
                text-transform:uppercase;margin:16px 0 0;">The Anecdote</p>
      {anecdote_block}
      {tags_block}
      <a href="{p['url']}"
         style="display:inline-block;margin-top:18px;font-size:14px;
                color:#2563eb;text-decoration:none;font-weight:500;">
        Read the full biography &rarr;
      </a>
    </div>"""


def build_email_html(people: list, date_display: str,
                     obituaries: list = None) -> str:
    cards = "".join(_card(p) for p in people)

    # v9: Obituary section (optional)
    obit_section = ""
    if obituaries:
        obit_cards = "".join(_obituary_card(o) for o in obituaries)
        obit_section = f"""
    <div style="text-align:center;border-bottom:2px solid #e5e0d8;border-top:2px solid #e5e0d8;
                padding:24px 0;margin:32px 0;">
      <h2 style="font-size:22px;font-weight:700;color:#8b5e3c;margin:0;line-height:1.2;">
        Obituary Digest</h2>
      <p style="font-size:13px;color:#6b7280;margin:8px 0 0;">
        Notable lives remembered this week &mdash; from The New York Times &amp; The Guardian</p>
    </div>
    {obit_cards}"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="margin:0;padding:0;background:#faf9f7;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',
             Roboto,'Helvetica Neue',Arial,sans-serif;">
  <div style="max-width:640px;margin:0 auto;padding:30px 16px 50px;">
    <div style="text-align:center;border-bottom:2px solid #e5e0d8;
                padding-bottom:24px;margin-bottom:32px;">
      <p style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;
                color:#9ca3af;margin:0 0 8px;">Daily Digest</p>
      <h1 style="font-size:26px;font-weight:700;color:#2e6e4e;margin:0;line-height:1.2;">
        Wikipedia Biographical Digest</h1>
      <p style="font-size:14px;color:#6b7280;margin:9px 0 0;">
        {date_display} &mdash; Four lives worth knowing about</p>
    </div>
    {cards}
    {obit_section}
    <p style="text-align:center;font-size:12px;color:#9ca3af;
              margin-top:12px;border-top:1px solid #e5e0d8;padding-top:20px;">
      Generated automatically &bull; Sources: Wikipedia, NYT, The Guardian &bull; {date_display}
    </p>
  </div>
</body>
</html>"""


def send_email(subject: str, html_body: str) -> None:
    """Send the digest via Gmail SMTP using an App Password."""
    if not GMAIL_EMAIL or not GMAIL_APP_PASSWORD:
        log.error("GMAIL_EMAIL or GMAIL_APP_PASSWORD secret is not set. Aborting.")
        sys.exit(1)
    if not RECIPIENT:
        log.error("DIGEST_RECIPIENT secret is not set. Aborting.")
        sys.exit(1)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Wikipedia Digest <{GMAIL_EMAIL}>"
    msg["To"]      = RECIPIENT
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    log.info("Connecting to Gmail SMTP (%s:%d)…", SMTP_HOST, SMTP_PORT)
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(GMAIL_EMAIL, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_EMAIL, RECIPIENT, msg.as_string())
    log.info("Email sent successfully to %s.", RECIPIENT)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    today        = datetime.date.today()
    month_name   = today.strftime("%B")         # "February"
    month_num    = today.strftime("%m")          # "02" (zero-padded, as API expects)
    day          = today.strftime("%-d")         # "25" (no leading zero)
    day_padded   = today.strftime("%d")          # "25" (zero-padded for API)
    date_display = today.strftime("%-d %B %Y")
    date_str     = today.strftime("%Y-%m-%d")

    log.info("=== Wikipedia Biographical Digest v9.2 starting for %s ===", date_str)

    # ── v9.2: Load seen-items registry so we never repeat a bio or obituary ──
    seen = load_seen()
    seen_wiki_titles = {e["title"] for e in seen.get("wikipedia", [])}
    seen_obit_urls   = {e["url"]   for e in seen.get("obituaries", [])}
    log.info(
        "Seen filter active — blocking %d Wikipedia titles, %d obituary URLs",
        len(seen_wiki_titles), len(seen_obit_urls),
    )

    candidates = fetch_candidates(month_name, month_num, day_padded)
    if not candidates:
        log.error("No person candidates found. Aborting.")
        sys.exit(1)

    # Filter out previously-seen Wikipedia bios
    fresh_candidates = [c for c in candidates if c["title"] not in seen_wiki_titles]
    skipped_count    = len(candidates) - len(fresh_candidates)
    log.info(
        "Candidates: %d total, %d already seen (skipped), %d fresh",
        len(candidates), skipped_count, len(fresh_candidates),
    )

    # Safety net: if filtering removed too many, fall back to full pool
    # (this prevents the digest failing on dates with very few entries)
    if len(fresh_candidates) < 4:
        log.warning(
            "Only %d fresh candidates after seen-filter — relaxing filter for this run.",
            len(fresh_candidates),
        )
        fresh_candidates = candidates   # use full pool this run

    scored = []
    for candidate in fresh_candidates:
        title = candidate["title"]
        log.info("Fetching biography: %s", title)
        extract = get_biography(title)

        if not extract or len(extract) < 400:
            log.info("Skipping %s — biography too short or missing", title)
            continue

        birth_year, death_year = extract_years_from_bio(
            extract, candidate["source"], candidate["api_year"]
        )
        candidate["birth_year"] = birth_year
        candidate["death_year"] = death_year

        score = score_biography(extract)

        # Build labelled snippets (v8 anecdote format)
        anecdote_snippets = extract_anecdote(extract, score["signals"])

        candidate.update({
            "extract":           extract,
            "score":             score,
            "signals":           score["signals"],
            "tagline":           clean_tagline(candidate["description"], extract),
            "anecdote_snippets": anecdote_snippets,
            # Plain-text anecdote kept for compatibility
            "anecdote":          " ".join(s["text"] for s in anecdote_snippets),
            "url":               "https://en.wikipedia.org/wiki/" + urllib.parse.quote(
                                     title.replace(" ", "_")
                                 ),
        })
        scored.append(candidate)
        time.sleep(0.4)

    if not scored:
        log.error("No scoreable biographies found. Aborting.")
        sys.exit(1)

    log.info("Scored %d biographies", len(scored))

    # v9.3: Stronger randomisation so day-to-day AND intra-day runs vary meaningfully.
    # uniform(0, 8) is wide enough to regularly re-order the ranked pool.
    for p in scored:
        p["_rand_score"] = p["score"]["total"] + random.uniform(0, 8)
    ranked = sorted(scored, key=lambda x: -x["_rand_score"])

    # Prefer candidates with rich personal anecdotes; defer dry ones
    rich_pool = [p for p in ranked if has_rich_anecdote(p)]
    dry_pool  = [p for p in ranked if not has_rich_anecdote(p)]
    log.info(
        "Anecdote quality — rich: %d, dry (deferred): %d",
        len(rich_pool), len(dry_pool)
    )

    ordered  = rich_pool + dry_pool
    selected = select_four(ordered)
    log.info("Selected: %s", [p["name"] for p in selected])

    # ── Obituary section (fault-tolerant) ────────────────────────────────
    obituaries = []
    try:
        log.info("=== Obituary Digest starting ===")
        obit_candidates = fetch_obituaries()

        # Filter out previously-seen obituary URLs
        fresh_obits = [
            o for o in obit_candidates
            if o.get("link", o.get("archive_url", "")) not in seen_obit_urls
        ]
        skipped_obits = len(obit_candidates) - len(fresh_obits)
        log.info(
            "Obituary candidates: %d total, %d already seen, %d fresh",
            len(obit_candidates), skipped_obits, len(fresh_obits),
        )

        if fresh_obits:
            obituaries = select_obituaries(fresh_obits)
            log.info("Obituary section: %d entries selected", len(obituaries))
        else:
            log.warning("No fresh obituary candidates after seen-filter; skipping section")
    except Exception as exc:
        log.error("Obituary section failed (non-fatal): %s", exc)
        obituaries = []

    html    = build_email_html(selected, date_display,
                               obituaries=obituaries if obituaries else None)
    subject = f"Wikipedia Biographical Digest — {date_display}"
    send_email(subject, html)

    # ── v9.2: Persist what we sent so it won't recur ─────────────────────
    new_wiki  = [p["title"] for p in selected]
    new_obits = [
        (o.get("link", o.get("archive_url", "")), o.get("name", ""))
        for o in obituaries
    ]
    save_seen(seen, new_wiki, new_obits)

    obit_note = f" + {len(obituaries)} obituaries" if obituaries else ""
    print(f"Done. Digest sent for {date_display}{obit_note}.")


if __name__ == "__main__":
    main()
