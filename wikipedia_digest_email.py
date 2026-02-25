#!/usr/bin/env python3
"""
Wikipedia Biographical Digest — GitHub Actions / Email Edition
Uses Wikipedia's structured On This Day REST API (births + deaths only)
to ensure every candidate is a real person. Scores each biography against
editorial criteria and emails the top 4 as a styled HTML digest.
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

# ── Logging (stdout is captured by GitHub Actions) ───────────────────────────
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

# ── Email config — set via GitHub Secrets ────────────────────────────────────
SMTP_USER     = os.environ.get("YAHOO_EMAIL", "")
SMTP_PASSWORD = os.environ.get("YAHOO_APP_PASSWORD", "")
RECIPIENT     = os.environ.get("DIGEST_RECIPIENT", SMTP_USER)
SMTP_HOST     = "smtp.mail.yahoo.com"
SMTP_PORT     = 587

# ── Anecdote word limit ───────────────────────────────────────────────────────
ANECDOTE_MAX_WORDS = 750


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helper
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": "WikipediaBiographicalDigest/2.0 (personal digest; private user)",
    "Accept": "application/json",
}


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

def fetch_candidates(month: str, day: str) -> list:
    """
    Calls the Wikipedia REST API for births and deaths on this date.
    Returns only entries that are confirmed individual people.
    Each dict includes: name, title, description, birth_year, death_year, source.
    """
    candidates = []
    seen = set()

    for category in ("births", "deaths"):
        url = (
            f"https://en.wikipedia.org/api/rest_v1/feed/onthisday"
            f"/{category}/{month}/{day}"
        )
        data = http_get_json(url)
        entries = data.get(category, [])
        log.info("On This Day API — %s: %d raw entries", category, len(entries))

        for entry in entries:
            year  = entry.get("year")
            pages = entry.get("pages", [])

            for page in pages:
                title       = page.get("title", "").strip()
                description = page.get("description", "").strip()

                if not title or title in seen:
                    continue

                # Only include confirmed individual people
                if not _is_person(title, description):
                    log.info("Skipping non-person: %s (%s)", title, description)
                    continue

                seen.add(title)
                birth_year, death_year = _extract_years(category, year, description)

                candidates.append({
                    "name":        page.get("normalizedtitle", title),
                    "title":       title,
                    "description": description,
                    "birth_year":  birth_year,
                    "death_year":  death_year,
                    "source":      category,
                })

        time.sleep(0.5)

    log.info("Total confirmed person candidates: %d", len(candidates))
    return candidates


# ── Person filter ─────────────────────────────────────────────────────────────

_PERSON_SIGNALS = [
    r'\bactor\b', r'\bactress\b', r'\bauthor\b', r'\bwriter\b', r'\bpoet\b',
    r'\bnovelist\b', r'\bplaywright\b', r'\bjournalist\b', r'\beditor\b',
    r'\bpolitician\b', r'\bpresident\b', r'\bprime minister\b', r'\bsenator\b',
    r'\bking\b', r'\bqueen\b', r'\bprince\b', r'\bprincess\b', r'\bmonarch\b',
    r'\bgeneral\b', r'\badmiral\b', r'\bcolonel\b', r'\bcommander\b',
    r'\bscientist\b', r'\bphysicist\b', r'\bchemist\b', r'\bbiologist\b',
    r'\bmathematician\b', r'\bastronomer\b', r'\bgeologist\b',
    r'\bphilosopher\b', r'\btheologian\b', r'\barchbishop\b', r'\bbishop\b',
    r'\bcomposer\b', r'\bmusician\b', r'\bsinger\b', r'\bpianist\b',
    r'\bpainter\b', r'\bartist\b', r'\bsculptor\b', r'\barchitect\b',
    r'\bphotographer\b', r'\bdirector\b', r'\bproducer\b', r'\bfilmmaker\b',
    r'\binventor\b', r'\bengineer\b', r'\bexplorer\b', r'\baviator\b',
    r'\bpilot\b', r'\bastronaut\b', r'\bcosmonaute?\b',
    r'\bathlete\b', r'\bfootballer\b', r'\bboxer\b', r'\bcricketer\b',
    r'\btennis player\b', r'\bcyclist\b', r'\bswimmer\b', r'\bjockey\b',
    r'\beconomist\b', r'\bpsychologist\b', r'\bsociologist\b',
    r'\bhistorian\b', r'\barchaeologist\b', r'\banthropologist\b',
    r'\bmagician\b', r'\bcomedian\b', r'\bhumorist\b', r'\bsatirist\b',
    r'\bactivist\b', r'\breformer\b', r'\brevolutionary\b', r'\bdissident\b',
    r'\bnobel\b', r'\born\b', r'\bdied\b',
    r'\bamerican\b', r'\bbritish\b', r'\benglish\b', r'\bscottish\b',
    r'\birish\b', r'\bwelsh\b', r'\bfrench\b', r'\bgerman\b',
    r'\bitalian\b', r'\bspanish\b', r'\brussian\b', r'\bindian\b',
    r'\bchina\b', r'\bjapanese\b', r'\baustralian\b', r'\bcanadian\b',
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
    r'\bmassacre\b', r'\battack\b', r'\bbombing\b',
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

    # Fallback: 2–4 capitalised words with no obvious non-person tokens
    parts = title.split()
    if 2 <= len(parts) <= 4 and all(p[0].isupper() for p in parts if p):
        stopwords = {"of", "the", "and", "in", "at", "by", "for", "to", "on"}
        if not any(p.lower() in stopwords for p in parts):
            return True

    return False


def _extract_years(category: str, year, description: str):
    """
    For 'births' entries, year IS the birth year.
    For 'deaths' entries, year IS the death year.
    Tries to find the complementary year from the description.
    """
    birth_year = "?"
    death_year = "present"

    if category == "births":
        if year:
            birth_year = str(year)
        # Look for death year in description
        m = re.search(r'(?:died|d\.)\s*(\d{4})', description, re.IGNORECASE)
        if m:
            death_year = m.group(1)
        else:
            m2 = re.search(r'\d{4}[–\-](\d{4})', description)
            if m2:
                death_year = m2.group(1)

    elif category == "deaths":
        if year:
            death_year = str(year)
        # Look for birth year in description
        m = re.search(r'(?:born|b\.)\s*(\d{4})', description, re.IGNORECASE)
        if m:
            birth_year = m.group(1)
        else:
            m2 = re.search(r'(\d{4})[–\-]\d{4}', description)
            if m2:
                birth_year = m2.group(1)

    return birth_year, death_year


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Fetch full biography text from Wikipedia
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


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Score each biography against editorial criteria
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


def score_biography(extract: str) -> dict:
    if not extract or len(extract) < 400:
        return {"primary": 0, "secondary": 0, "total": 0, "signals": []}

    text_lower = extract.lower()
    primary = secondary = 0
    signals = []

    for criterion, patterns in _PRIMARY_SIGNALS.items():
        if sum(1 for p in patterns if re.search(p, text_lower)) >= 2:
            primary += 1
            signals.append(criterion)

    for criterion, patterns in _SECONDARY_SIGNALS.items():
        if sum(1 for p in patterns if re.search(p, text_lower)) >= 2:
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
# Step 4 — Extract one-liner and anecdote
# ─────────────────────────────────────────────────────────────────────────────

def extract_one_liner(api_description: str, extract: str) -> str:
    """
    Returns a single clean sentence summarising who the person is.
    Prefers the Wikipedia API description (it's editorially curated and short).
    Falls back to the opening sentence of the biography.
    """
    if api_description and len(api_description) > 20:
        desc = api_description.strip()
        desc = desc[0].upper() + desc[1:]
        if not desc.endswith("."):
            desc += "."
        return desc

    sentences = re.split(r'(?<=[.!?])\s+', extract.strip())
    for sent in sentences[:4]:
        sent = sent.strip()
        if len(sent) > 40:
            return sent
    return sentences[0].strip() if sentences else ""


def extract_anecdote(extract: str, signals: list) -> str:
    """
    Finds the richest passage in the biography and returns up to
    ANECDOTE_MAX_WORDS words of it, ending on a clean sentence boundary.
    Targets paragraphs dense with the scored signals for maximum interest.
    """
    all_signals = {**_PRIMARY_SIGNALS, **_SECONDARY_SIGNALS}
    scoring_patterns = []
    for sig in signals:
        scoring_patterns.extend(all_signals.get(sig, []))
    if not scoring_patterns:
        for patterns in _SECONDARY_SIGNALS.values():
            scoring_patterns.extend(patterns)

    # Split into sentences and score each one
    sentences = [s.strip() for s in re.split(r'(?<=[.!?])\s+', extract.strip())]
    sentences = [s for s in sentences if len(s) > 35]

    if not sentences:
        return ""

    scored = []
    for i, sent in enumerate(sentences):
        sent_lower = sent.lower()
        hits = sum(1 for p in scoring_patterns if re.search(p, sent_lower))
        # Favour the middle of the article — intros are biographical facts,
        # section ends are often references
        pos = i / max(len(sentences), 1)
        pos_bonus = 1.0 if 0.10 < pos < 0.85 else 0.0
        scored.append((hits + pos_bonus, i, sent))

    scored.sort(key=lambda x: (-x[0], x[1]))
    best_idx = scored[0][1]

    # Build a window starting just before the best sentence
    # and extending forward until we approach the word limit
    start = max(0, best_idx - 1)
    words_so_far = 0
    window = []

    for i in range(start, len(sentences)):
        sent = sentences[i]
        word_count = len(sent.split())
        if words_so_far + word_count > ANECDOTE_MAX_WORDS and window:
            break
        window.append(sent)
        words_so_far += word_count
        if words_so_far >= ANECDOTE_MAX_WORDS:
            break

    return " ".join(window).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — Select 4 with era diversity
# ─────────────────────────────────────────────────────────────────────────────

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
    if len(ranked) <= 4:
        return ranked

    selected = []
    era_counts = {}

    for p in ranked:
        e = _era(p["birth_year"])
        if era_counts.get(e, 0) >= 2 and len(ranked) > 6:
            continue
        selected.append(p)
        era_counts[e] = era_counts.get(e, 0) + 1
        if len(selected) == 4:
            break

    # Fill any remaining slots if diversity filter left us short
    if len(selected) < 4:
        for p in ranked:
            if p not in selected:
                selected.append(p)
            if len(selected) == 4:
                break

    return selected[:4]


# ─────────────────────────────────────────────────────────────────────────────
# Step 6 — Build and send the HTML email
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
    month        = today.strftime("%B")
    day          = today.strftime("%-d")   # no leading zero, e.g. "7" not "07"
    date_display = today.strftime("%-d %B %Y")
    date_str     = today.strftime("%Y-%m-%d")

    log.info("=== Wikipedia Biographical Digest v2 starting for %s ===", date_str)

    # 1. Fetch confirmed person candidates via the structured API
    candidates = fetch_candidates(month, day)
    if not candidates:
        log.error("No person candidates found. Aborting.")
        sys.exit(1)

    # 2. Fetch full biographies and score them
    scored = []
    for candidate in candidates:
        title = candidate["title"]
        log.info("Fetching biography: %s", title)
        extract = get_biography(title)

        if not extract or len(extract) < 400:
            log.info("Skipping %s — biography too short or missing", title)
            continue

        score = score_biography(extract)
        candidate.update({
            "extract":  extract,
            "score":    score,
            "signals":  score["signals"],
            "tagline":  extract_one_liner(candidate["description"], extract),
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

    # 3. Rank and select 4 with era diversity
    ranked   = sorted(scored, key=lambda x: -x["score"]["total"])
    selected = select_four(ranked)
    log.info("Selected: %s", [p["name"] for p in selected])

    # 4. Build and send the email
    html    = build_email_html(selected, date_display)
    subject = f"Wikipedia Biographical Digest — {date_display}"
    send_email(subject, html)
    print(f"Done. Digest sent for {date_display}.")


if __name__ == "__main__":
    main()
