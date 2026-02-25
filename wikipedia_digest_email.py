#!/usr/bin/env python3
"""
Wikipedia Biographical Digest — GitHub Actions / Email Edition v8

Changes from v7:
  - Anecdote: 2-3 labelled snippets from distinct sections (~500 words total)
  - Swap-out: dry biographies (no personal content) replaced by richer candidates
  - Year extraction: strip IPA pronunciations + wider regex window (fixes Rothko-style errors)
  - Taglines: year patterns stripped (no redundant dates)
"""

import sys
import os
import re
import json
import time
import logging
import datetime
import smtplib
import urllib.request
import urllib.parse
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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
    "User-Agent": "WikipediaBiographicalDigest/8.0 (personal digest; private user)",
    "Accept": "application/json",
}


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
    Improved year extraction (v8):
    - Strips IPA pronunciation guides e.g. /ˈrɒθkoʊ/ from opening parenthetical
    - Strips square-bracketed annotations e.g. [O.S. 3 April] [Russian: ...]
    - Tries simple (YYYY–YYYY) first for speed
    - Falls back to complex range with wider 100-char window (vs 60 in v7)
    - Extended born/died fallback search window (120 chars vs 80)
    """
    birth_year = "?"
    death_year = "present"

    head = extract[:600]

    # Strip IPA guide at start of parenthetical: (/ˈrɒθkoʊ/; → (
    head = re.sub(r'\(/[^/)]+/;?\s*', '(', head)
    # Strip square-bracketed content: [O.S. 3 April], [Russian: Маркус...]
    head = re.sub(r'\[[^\[\]]{0,200}\]', '', head)

    # 1. Simplest form: (YYYY–YYYY) or (YYYY-YYYY)
    simple_m = re.search(
        r'\(\s*(\b(?:1[0-9]{3}|20[0-9]{2})\b)\s*[–\-]\s*(\b(?:1[0-9]{3}|20[0-9]{2})\b)\s*\)',
        head
    )
    if simple_m:
        birth_year = simple_m.group(1)
        death_year = simple_m.group(2)
        return birth_year, death_year

    # 2. Complex range: (DATE YYYY – DATE YYYY) — wider 100-char window
    range_m = re.search(
        r'\(\s*[^()]{0,100}?(\b(?:1[0-9]{3}|20[0-9]{2})\b)[^()]{0,60}?[–\-]\s*[^()]{0,60}?(\b(?:1[0-9]{3}|20[0-9]{2})\b)\s*\)',
        head
    )
    if range_m:
        birth_year = range_m.group(1)
        death_year = range_m.group(2)
        return birth_year, death_year

    # 3. Use API year for the known event
    if category == "births" and api_year:
        birth_year = str(api_year)
    elif category == "deaths" and api_year:
        death_year = str(api_year)

    # 4. Text pattern search with extended windows
    born_m = re.search(
        r'\bborn\b[^.]{0,120}?\b(1[0-9]{3}|20[0-9]{2})\b',
        extract[:1500], re.IGNORECASE
    )
    if born_m and birth_year == "?":
        birth_year = born_m.group(1)

    died_m = re.search(
        r'\bdied\b[^.]{0,80}?\b(1[0-9]{3}|20[0-9]{2})\b',
        extract[:2000], re.IGNORECASE
    )
    if died_m:
        death_year = died_m.group(1)

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
    ],
    "defiance": [
        r'\brefused\b', r'\bdefied\b', r'\bresist\w+\b',
        r'\bfirst woman\b', r'\bfirst black\b', r'\bfirst african\b',
        r'\bfirst person\b', r'\bpersist\w+\b', r'\bcourage\b',
        r'\bbroke\b', r'\bbarrier\b',
    ],
    "heretic": [
        r'\bdismiss\w+\b', r'\bscoff\w+\b', r'\bvindicat\w+\b',
        r'\bproved.*wrong\b', r'\bskeptic\w*\b', r'\bcontroversi\w+\b',
        r'\bunconventional\b', r'\bmocked\b', r'\bridiculed\b',
    ],
    "humanising": [
        r'\bfeared\b', r'\bcried\b', r'\blaughed\b', r'\bfamily\b',
        r'\bfriend\w*\b', r'\bhumble\b', r'\bmodest\b', r'\bquiet\w*\b',
        r'\bshy\b', r'\bloved\b', r'\bdevoted\b',
    ],
    "irony": [
        r'\bironical?ly?\b', r'\bparadox\b', r'\bunexpected\b',
        r'\btwist\b', r'\bdespite\b', r'\bnevertheless\b',
        r'\bcuriously\b', r'\bof all people\b',
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
    desc = re.sub(r'\s{2,}', ' ', desc).strip()
    # Clean trailing space before punctuation
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

    for header, text in pool:
        if len(snippets) >= 3 or total_words >= 500:
            break

        sentences = [
            s.strip() for s in re.split(r'(?<=[.!?])\s+', text)
            if len(s.strip()) > 35 and not _is_cultural_sentence(s)
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

        snippets.append({"label": label, "text": " ".join(window)})
        total_words += wc

    if not snippets:
        # Ultimate fallback: take up to 400 words from non-cultural extract text
        all_sents = [
            s.strip() for s in re.split(r'(?<=[.!?])\s+', extract)
            if len(s.strip()) > 35 and not _is_cultural_sentence(s)
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
    "watercooler":  "Watercooler Anecdote",
    "mental_model": "Eccentric Mental Model",
    "last_of_kind": "Last of a Kind",
    "underdog":     "Underdog / Overlooked",
    "diy":          "Hidden Origin / DIY",
    "defiance":     "Strategic Defiance",
    "heretic":      "Scientific Heretic",
    "humanising":   "Humanising Contrast",
    "irony":        "Narrative Irony",
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


def build_email_html(people: list, date_display: str) -> str:
    cards = "".join(_card(p) for p in people)
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
    <p style="text-align:center;font-size:12px;color:#9ca3af;
              margin-top:12px;border-top:1px solid #e5e0d8;padding-top:20px;">
      Generated automatically &bull; Source: Wikipedia &bull; {date_display}
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

    log.info("=== Wikipedia Biographical Digest v8 starting for %s ===", date_str)

    candidates = fetch_candidates(month_name, month_num, day_padded)
    if not candidates:
        log.error("No person candidates found. Aborting.")
        sys.exit(1)

    scored = []
    for candidate in candidates:
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
    ranked = sorted(scored, key=lambda x: -x["score"]["total"])

    # v8: Prefer candidates with rich personal anecdotes; defer dry ones
    rich_pool = [p for p in ranked if has_rich_anecdote(p)]
    dry_pool  = [p for p in ranked if not has_rich_anecdote(p)]
    log.info(
        "Anecdote quality — rich: %d, dry (deferred): %d",
        len(rich_pool), len(dry_pool)
    )

    # Rich candidates ranked first; dry ones fill any remaining slots
    ordered  = rich_pool + dry_pool
    selected = select_four(ordered)
    log.info("Selected: %s", [p["name"] for p in selected])

    html    = build_email_html(selected, date_display)
    subject = f"Wikipedia Biographical Digest — {date_display}"
    send_email(subject, html)
    print(f"Done. Digest sent for {date_display}.")


if __name__ == "__main__":
    main()
