#!/usr/bin/env python3
"""
Wikipedia Biographical Digest — GitHub Actions / Email Edition v4
Uses Wikipedia's structured On This Day REST API (births + deaths only).
Section-aware anecdote extraction skips cultural legacy / political analysis
sections and targets personal life, character, and early life sections instead.
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

SMTP_USER     = os.environ.get("YAHOO_EMAIL", "")
SMTP_PASSWORD = os.environ.get("YAHOO_APP_PASSWORD", "")
RECIPIENT     = os.environ.get("DIGEST_RECIPIENT", SMTP_USER)
SMTP_HOST     = "smtp.mail.yahoo.com"
SMTP_PORT     = 587

ANECDOTE_MAX_WORDS = 750

HEADERS = {
    "User-Agent": "WikipediaBiographicalDigest/4.0 (personal digest; private user)",
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
    Primary source: Wikipedia REST API (official numeric month format).
    Fallback: Wikipedia Action API parsing the date article's Births/Deaths sections.
    """
    candidates = []
    seen = set()

    # ── Primary: REST API (numeric month, zero-padded) ───────────────────────
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

    # ── Fallback: Action API — parse the date article ─────────────────────────
    log.warning("REST API returned no data; falling back to Action API date article")
    # Use month name + unpadded day for the Wikipedia article title, e.g. "February_25"
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

    # Parse Births and Deaths sections for linked person entries
    # Format: "* [[Year]] – [[Person Name]], description (born/died YYYY)"
    link_pattern = re.compile(r'\[\[([^\|\]#]+)(?:\|[^\]]+)?\]\]')
    year_pattern  = re.compile(r'^\*\s*(\d{4})')

    in_section = False
    section_type = "births"

    for line in wikitext.splitlines():
        lower = line.lower().strip()

        # Detect section headers
        if re.match(r'==\s*births\s*==', lower):
            in_section = True
            section_type = "births"
            continue
        elif re.match(r'==\s*deaths\s*==', lower):
            in_section = True
            section_type = "deaths"
            continue
        elif re.match(r'==\s*\w', lower) and in_section:
            in_section = False
            continue

        if not in_section:
            continue

        # Extract year from line
        year_m = year_pattern.match(line)
        year   = int(year_m.group(1)) if year_m else None

        # Extract linked titles from line
        links = link_pattern.findall(line)
        for linked in links:
            linked = linked.strip()
            # Skip year articles (just digits)
            if re.match(r'^\d+$', linked):
                continue
            if linked in seen:
                continue
            # Basic person check
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
    Extract birth and death years reliably by looking at the opening
    parenthetical of the biography, e.g. '(15 April 1894 – 11 September 1971)'.
    Falls back to searching the text for born/died patterns.
    """
    birth_year = "?"
    death_year = "present"

    # Best source: parenthetical year range at start of article
    # Handles formats like "(1894–1971)", "(April 1894 – September 1971)",
    # "(15 April [O.S. 3 April] 1894 – 11 September 1971)"
    range_m = re.search(
        r'\(\s*[^()]{0,60}?(\b1[0-9]{3}|20[0-9]{2}\b)[^()]{0,60}?'
        r'[–\-]\s*[^()]{0,60}?(\b1[0-9]{3}|20[0-9]{2}\b)\s*\)',
        extract[:500]
    )
    if range_m:
        birth_year = range_m.group(1)
        death_year = range_m.group(2)
        return birth_year, death_year

    # Second source: use the API year for the known category
    if category == "births" and api_year:
        birth_year = str(api_year)
    elif category == "deaths" and api_year:
        death_year = str(api_year)

    # Third source: search text for "born YYYY" / "died YYYY"
    born_m = re.search(r'\bborn\b[^.]{0,80}?\b(1[0-9]{3}|20[0-9]{2})\b',
                       extract[:1000], re.IGNORECASE)
    if born_m and birth_year == "?":
        birth_year = born_m.group(1)

    died_m = re.search(r'\bdied\b[^.]{0,80}?\b(1[0-9]{3}|20[0-9]{2})\b',
                       extract[:2000], re.IGNORECASE)
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

# Raise thresholds: primary needs 4 hits, secondary needs 3 hits
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
    Returns a clean single-sentence description of the person.
    Uses the Wikipedia API description (always short and curated) first.
    Falls back to the first sentence of the biography, stripped of
    parenthetical dates, pronunciations, and Old Style date markers.
    """
    if api_description and len(api_description) > 15:
        desc = api_description.strip()
        desc = desc[0].upper() + desc[1:]
        if not desc.endswith("."):
            desc += "."
        return desc

    # Fallback: strip all parentheticals from the first paragraph first
    # (handles nested patterns like "(15 April [O.S. 3 April] 1894 – 1971)"),
    # then split into sentences and return the first clean one.
    first_para = extract.split("\n\n")[0] if "\n\n" in extract else extract[:800]
    cleaned_para = first_para
    for _ in range(6):
        cleaned_para = re.sub(r'\[[^\[\]]*\]', '', cleaned_para)
        cleaned_para = re.sub(r'\([^()]*\)', '', cleaned_para)
    cleaned_para = re.sub(r'\s{2,}', ' ', cleaned_para).strip()
    sentences = re.split(r'(?<=[.!?])\s+', cleaned_para)
    for sent in sentences[:4]:
        sent = sent.strip()
        if len(sent) > 40:
            return sent
    return cleaned_para[:200] if cleaned_para else ""


# ── Section classification ────────────────────────────────────────────────────

# Sections to skip entirely when building the anecdote
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

# Sections to actively prefer when building the anecdote
_PREFER_SECTION_KEYWORDS = {
    "personal life", "private life", "early life", "childhood",
    "early years", "youth", "education", "upbringing",
    "character", "personality", "personal beliefs", "religion",
    "family", "marriages", "relationships", "health",
    "later life", "later years", "death", "final years",
    "anecdotes", "personal", "private",
}


def _section_score(header: str) -> float:
    """
    Returns a multiplier for sentences in this section.
    0.0 = skip entirely. 1.0 = normal. 2.0 = preferred.
    """
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
    Section headers are detected as short lines (< 80 chars) that don't
    end with sentence punctuation, surrounded by blank lines.
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

        # A section header is a short, non-sentence line standing alone
        # or followed by content
        is_header = (
            len(first_line) > 0
            and len(first_line) < 80
            and not first_line[-1] in ".!?,;"
            and not re.match(r'^\d', first_line)  # doesn't start with a number
            and len(lines) == 1                    # standalone line
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


# ── Sentence-level cultural content filter ───────────────────────────────────

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


def extract_anecdote(extract: str, signals: list) -> str:
    """
    Section-aware anecdote extraction:
    1. Splits the biography into sections
    2. Skips cultural legacy / political analysis sections entirely
    3. Boosts sentences from personal life / character sections
    4. Scores remaining sentences against editorial signals
    5. Returns up to ANECDOTE_MAX_WORDS words from the richest passage
    """
    all_signals = {**_PRIMARY_SIGNALS, **_SECONDARY_SIGNALS}
    scoring_patterns = []
    for sig in signals:
        scoring_patterns.extend(all_signals.get(sig, []))
    if not scoring_patterns:
        for patterns in _SECONDARY_SIGNALS.values():
            scoring_patterns.extend(patterns)

    sections = _split_sections(extract)

    # Build a flat list of (sentence, section_multiplier, global_position)
    all_sentences = []
    total_section_chars = sum(len(s["text"]) for s in sections) or 1

    char_pos = 0
    for section in sections:
        mult = section["multiplier"]
        if mult == 0.0:
            char_pos += len(section["text"])
            continue  # Skip this section entirely

        raw_sentences = [
            s.strip()
            for s in re.split(r'(?<=[.!?])\s+', section["text"])
            if len(s.strip()) > 35
        ]
        for sent in raw_sentences:
            if _is_cultural_sentence(sent):
                continue
            all_sentences.append((sent, mult, char_pos / total_section_chars))
            char_pos += len(sent)

    if not all_sentences:
        # Fallback: use full extract without section filtering
        raw = [s.strip() for s in re.split(r'(?<=[.!?])\s+', extract) if len(s.strip()) > 35]
        all_sentences = [(s, 1.0, i / max(len(raw), 1)) for i, s in enumerate(raw)]

    # Score each sentence
    scored = []
    for idx, (sent, mult, pos) in enumerate(all_sentences):
        sent_lower = sent.lower()
        hits = sum(1 for p in scoring_patterns if re.search(p, sent_lower))
        # Small position bonus for middle of article
        pos_bonus = 0.5 if 0.10 < pos < 0.85 else 0.0
        scored.append((hits * mult + pos_bonus, idx, sent))

    scored.sort(key=lambda x: (-x[0], x[1]))
    best_idx = scored[0][1]

    # Build window around best sentence up to word limit
    start = max(0, best_idx - 1)
    words_so_far = 0
    window = []

    for i in range(start, len(all_sentences)):
        sent = all_sentences[i][0]
        wc   = len(sent.split())
        if words_so_far + wc > ANECDOTE_MAX_WORDS and window:
            break
        window.append(sent)
        words_so_far += wc
        if words_so_far >= ANECDOTE_MAX_WORDS:
            break

    return " ".join(window).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — Select 4 with era AND field/nationality diversity
# ─────────────────────────────────────────────────────────────────────────────

def _diversity_key(candidate: dict) -> str:
    """
    Returns a short diversity key combining nationality + broad field,
    extracted from the API description.
    e.g. "Argentine politician", "Soviet statesman" → "argentine_politics"
    """
    desc = candidate.get("description", "").lower()

    # Nationality
    nationalities = [
        "american", "british", "english", "scottish", "irish", "welsh",
        "french", "german", "italian", "spanish", "russian", "soviet",
        "chinese", "japanese", "indian", "australian", "canadian",
        "argentine", "brazilian", "nigerian", "south african", "egyptian",
        "polish", "dutch", "swedish", "norwegian", "greek", "turkish",
        "mexican", "cuban", "venezuelan", "colombian", "chilean",
    ]
    nationality = next((n for n in nationalities if n in desc), "other")

    # Broad field
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

    selected      = []
    era_counts    = {}
    div_key_used  = set()

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

    return f"""
    <div style="background:#ffffff;border:1px solid #e5e0d8;border-radius:12px;
                padding:24px 26px 20px;margin-bottom:24px;">
      <p style="font-size:11px;font-weight:600;color:#9ca3af;text-transform:uppercase;
                letter-spacing:.09em;margin:0 0 7px;">{source}</p>
      <div style="margin-bottom:12px;">
        <span style="font-size:20px;font-weight:700;color:#1a1a1a;">{p['name']}</span>
        <span style="font-size:13px;color:#6b7280;margin-left:8px;">{years}</span>
        <p style="font-size:14px;color:#555;font-style:italic;margin:6px 0 0;">{p['tagline']}</p>
      </div>
      <p style="font-size:11px;font-weight:700;color:#2e6e4e;letter-spacing:.1em;
                text-transform:uppercase;margin:16px 0 7px;">The Anecdote</p>
      <p style="font-size:15px;line-height:1.78;color:#2d2d2d;margin:0;">{p['anecdote']}</p>
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
    if not SMTP_USER or not SMTP_PASSWORD:
        log.error("Credentials missing. Set YAHOO_EMAIL and YAHOO_APP_PASSWORD secrets.")
        sys.exit(1)
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = RECIPIENT
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    log.info("Sending email to %s…", RECIPIENT)
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, RECIPIENT, msg.as_string())
    log.info("Email sent successfully.")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    today        = datetime.date.today()
    month_name   = today.strftime("%B")        # "February"
    month_num    = today.strftime("%m")         # "02"  (zero-padded, as API expects)
    day          = today.strftime("%-d")        # "25"  (no leading zero)
    day_padded   = today.strftime("%d")         # "25"  (zero-padded for API)
    date_display = today.strftime("%-d %B %Y")
    date_str     = today.strftime("%Y-%m-%d")

    log.info("=== Wikipedia Biographical Digest v4 starting for %s ===", date_str)

    candidates = fetch_candidates(month_name, month_num, day_padded)
    # day (unpadded) is used inside the fallback for the article title
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

        # Extract years from biography text (most reliable source)
        birth_year, death_year = extract_years_from_bio(
            extract, candidate["source"], candidate["api_year"]
        )
        candidate["birth_year"] = birth_year
        candidate["death_year"] = death_year

        score = score_biography(extract)
        candidate.update({
            "extract":  extract,
            "score":    score,
            "signals":  score["signals"],
            "tagline":  clean_tagline(candidate["description"], extract),
            "anecdote": extract_anecdote(extract, score["signals"]),
            "url":      "https://en.wikipedia.org/wiki/" + urllib.parse.quote(
                            title.replace(" ", "_")
                        ),
        })
        scored.append(candidate)
        time.sleep(0.4)

    if not scored:
        log.error("No scoreable biographies found. Aborting.")
        sys.exit(1)

    log.info("Scored %d biographies", len(scored))
    ranked   = sorted(scored, key=lambda x: -x["score"]["total"])
    selected = select_four(ranked)
    log.info("Selected: %s", [p["name"] for p in selected])

    html    = build_email_html(selected, date_display)
    subject = f"Wikipedia Biographical Digest — {date_display}"
    send_email(subject, html)
    print(f"Done. Digest sent for {date_display}.")


if __name__ == "__main__":
    main()
