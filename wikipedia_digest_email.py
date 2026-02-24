#!/usr/bin/env python3
"""
Wikipedia Biographical Digest — GitHub Actions / Email Edition
Runs daily via GitHub Actions, emails the digest as a styled HTML email.
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
import urllib.error
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# ── Logging to stdout (GitHub Actions captures this) ─────────────────────────
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

# ── Email config — set via GitHub Secrets / environment variables ─────────────
SMTP_USER     = os.environ.get("YAHOO_EMAIL", "")       # your Yahoo address
SMTP_PASSWORD = os.environ.get("YAHOO_APP_PASSWORD", "") # Yahoo app password
RECIPIENT     = os.environ.get("DIGEST_RECIPIENT", SMTP_USER)  # who gets the email

SMTP_HOST = "smtp.mail.yahoo.com"
SMTP_PORT = 587


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "WikipediaBiographicalDigest/1.0 "
        "(personal daily digest tool; contact: private user)"
    ),
    "Accept": "application/json",
}


def http_get_json(url: str, retries: int = 3, delay: float = 2.0) -> dict:
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
# Wikipedia API helpers
# ─────────────────────────────────────────────────────────────────────────────

WP_API = "https://en.wikipedia.org/w/api.php"


def get_page_extract(title: str) -> str:
    params = urllib.parse.urlencode({
        "action": "query",
        "titles": title,
        "prop": "extracts",
        "explaintext": "1",
        "exsectionformat": "plain",
        "format": "json",
        "formatversion": "2",
    })
    data = http_get_json(f"{WP_API}?{params}")
    pages = data.get("query", {}).get("pages", [])
    if not pages or pages[0].get("missing"):
        return ""
    return pages[0].get("extract", "")


def get_page_url(title: str) -> str:
    return "https://en.wikipedia.org/wiki/" + urllib.parse.quote(
        title.replace(" ", "_")
    )


def search_person_title(name: str) -> str:
    params = urllib.parse.urlencode({
        "action": "query",
        "list": "search",
        "srsearch": name,
        "srlimit": "3",
        "format": "json",
        "formatversion": "2",
    })
    data = http_get_json(f"{WP_API}?{params}")
    results = data.get("query", {}).get("search", [])
    return results[0]["title"] if results else ""


# ─────────────────────────────────────────────────────────────────────────────
# Candidate collection
# ─────────────────────────────────────────────────────────────────────────────

def get_on_this_day_people(month: str, day: str) -> list[dict]:
    candidates: list[dict] = []
    seen_titles: set[str] = set()

    # Source 1: Selected Anniversaries
    sa_title = f"Wikipedia:Selected_anniversaries/{month}_{day}"
    params = urllib.parse.urlencode({
        "action": "query",
        "titles": sa_title,
        "prop": "extracts",
        "explaintext": "1",
        "format": "json",
        "formatversion": "2",
    })
    data = http_get_json(f"{WP_API}?{params}")
    pages = data.get("query", {}).get("pages", [])
    if pages and not pages[0].get("missing"):
        _extract_names_from_text(
            pages[0].get("extract", ""), "anniversaries", candidates, seen_titles
        )

    # Source 2: Main Month_Day article (wikitext)
    params2 = urllib.parse.urlencode({
        "action": "query",
        "titles": f"{month}_{day}",
        "prop": "revisions",
        "rvprop": "content",
        "rvslots": "main",
        "format": "json",
        "formatversion": "2",
    })
    data2 = http_get_json(f"{WP_API}?{params2}")
    pages2 = data2.get("query", {}).get("pages", [])
    if pages2 and not pages2[0].get("missing"):
        wikitext = (
            pages2[0]
            .get("revisions", [{}])[0]
            .get("slots", {})
            .get("main", {})
            .get("content", "")
        )
        _extract_names_from_wikitext(wikitext, candidates, seen_titles)

    log.info("Collected %d unique candidates for %s %s", len(candidates), month, day)
    return candidates


def _extract_names_from_text(text, source, candidates, seen):
    pattern = re.compile(
        r'\b([A-Z][a-zÀ-ÿ\'-]+(?:\s+[A-Z][a-zÀ-ÿ\'-]+){1,3})\b'
    )
    for m in pattern.finditer(text):
        name = m.group(1)
        if _likely_person_name(name):
            title = search_person_title(name)
            if title and title not in seen:
                seen.add(title)
                candidates.append({"name": name, "title": title, "context": source})


def _extract_names_from_wikitext(wikitext, candidates, seen):
    link_pattern = re.compile(r'\[\[([^\|\]#]+)(?:\|[^\]]+)?\]\]')
    for line in wikitext.splitlines():
        for m in link_pattern.finditer(line):
            linked = m.group(1).strip()
            if " " in linked and linked[0].isupper() and _likely_person_name(linked):
                if linked not in seen:
                    seen.add(linked)
                    candidates.append({
                        "name": linked, "title": linked, "context": "date article"
                    })


_NON_PERSON_WORDS = {
    "January","February","March","April","May","June","July","August",
    "September","October","November","December","United States",
    "United Kingdom","Soviet Union","New York","World War","World Cup",
    "Nobel Prize","Olympic Games","Supreme Court","National","International",
    "European","American","British","French","German","Italian","Spanish",
    "Russian","Chinese","Japanese","Indian","Canadian","Australian",
    "South Africa","North America","South America","Middle East",
    "New Zealand","Hong Kong","San Francisco","Los Angeles",
    "This Day","On This","Selected Anniversaries",
}


def _likely_person_name(text: str) -> bool:
    if text in _NON_PERSON_WORDS:
        return False
    parts = text.split()
    if len(parts) < 2 or len(parts) > 5:
        return False
    if not all(p[0].isupper() for p in parts if p):
        return False
    small = {"of","the","and","in","at","by","for","to","with","on"}
    if any(p.lower() in small for p in parts):
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────

_PRIMARY_SIGNALS = {
    "watercooler": [
        r'\brefused\b', r'\binsisted\b', r'\bbetting\b', r'\bwager\b',
        r'\banecdote\b', r'\bleg(?:end|endary)\b', r'\brunning joke\b',
        r'\bpersonality\b', r'\bquirk\b', r'\binfamous\b', r'\bnotorious\b',
        r'\bsurpris\w+\b', r'\bunexpected\b', r'\bcurious\b', r'\bstrange\b',
        r'\bextraordinary\b', r'\bremarkabl\w+\b', r'\bincident\b',
        r'\bstory\b', r'\bcharacter\b', r'\bpersona\b', r'\bloved\b',
        r'\bhumour\b', r'\bhumor\b', r'\bwit\b', r'\bwhimsical\b',
        r'\bbizarre\b', r'\beccentric\b', r'\bodd\b', r'\bpeculiar\b',
        r'\bself-deprecat\w+\b',
    ],
    "mental_model": [
        r'\beccentric\b', r'\bhobby\b', r'\bhobbies\b',
        r'\bphilosophy\b', r'\bbelief\b', r'\bframework\b',
        r'\binvention\b', r'\bpatent\b', r'\btheory\b', r'\bmethod\b',
        r'\bsystem\b', r'\bapproach\b', r'\bparadigm\b', r'\bmodel\b',
        r'\bwish(?:es)?\b', r'\bfinal wish\b', r'\blast wish\b',
        r'\bwanted to be\b', r'\bbelieved that\b', r'\bdevised\b',
        r'\bpioneered\b', r'\bdeveloped\b', r'\bcreated\b',
        r'\bunique\b', r'\bunusual\b', r'\bunorthodox\b',
        r'\bself-taught\b', r'\bautodidact\b',
    ],
}

_SECONDARY_SIGNALS = {
    "last_of_kind":  [r'\blast\b',r'\bfinal\b',r'\bend of\b',r'\bextinct\b',r'\bvanish\w+\b',r'\bdisappear\w+\b',r'\bobsolete\b',r'\bno longer\b',r'\bforgotten\b'],
    "underdog":      [r'\bobscur\w+\b',r'\boverlooked\b',r'\bignored\b',r'\bunrecognis\w+\b',r'\bunrecogniz\w+\b',r'\bposthum\w+\b',r'\bonly after\b',r'\byears later\b',r'\bdespite\b',r'\bstruggl\w+\b',r'\bunsung\b'],
    "diy":           [r'\bself-taught\b',r'\bdropout\b',r'\baccident\w*\b',r'\bchance\b',r'\bgarage\b',r'\bbasement\b',r'\bhumble origin\b',r'\bno formal\b',r'\bwithout training\b'],
    "defiance":      [r'\brefused\b',r'\bdefied\b',r'\bresist\w+\b',r'\brebel\b',r'\bbarrier\b',r'\bpioneer\b',r'\bfirst woman\b',r'\bfirst black\b',r'\bfirst african\b',r'\bpersist\w+\b',r'\bcourage\b'],
    "heretic":       [r'\bdismiss\w+\b',r'\bscoff\w+\b',r'\bvindicat\w+\b',r'\bprove\w* wrong\b',r'\bskeptic\w*\b',r'\bcriticis\w+\b',r'\bcontroversi\w+\b',r'\bunconventional\b'],
    "humanising":    [r'\bloved\b',r'\bafraid\b',r'\bfear\w+\b',r'\bcried\b',r'\blaughed\b',r'\bchild\w*\b',r'\bfamily\b',r'\bfriend\w*\b',r'\bhumble\b',r'\bmodest\b',r'\bquiet\b',r'\bshy\b'],
    "irony":         [r'\birony\b',r'\bironic\b',r'\bparadox\b',r'\bunexpected\b',r'\btwist\b',r'\bsurpris\w+\b',r'\bcontrar\w+\b',r'\bdespite\b',r'\bnevertheless\b'],
}


def score_biography(extract: str) -> dict:
    if not extract or len(extract) < 300:
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
    length_bonus = min(len(extract) // 5000, 2)
    return {"primary": primary, "secondary": secondary,
            "total": primary * 10 + secondary * 2 + length_bonus, "signals": signals}


# ─────────────────────────────────────────────────────────────────────────────
# Biography data extraction
# ─────────────────────────────────────────────────────────────────────────────

def extract_life_years(extract: str, title: str) -> tuple[str, str]:
    birth = death = ""
    birth_m = re.search(r'\bborn\b[^.]{0,60}?\b(1[0-9]{3}|20[0-9]{2})\b', extract, re.IGNORECASE)
    if birth_m: birth = birth_m.group(1)
    death_m = re.search(r'\bdied\b[^.]{0,60}?\b(1[0-9]{3}|20[0-9]{2})\b', extract, re.IGNORECASE)
    if death_m: death = death_m.group(1)
    if not birth:
        range_m = re.search(r'\((\d{4})[–\-](\d{4}|\bpresent\b)\)', extract)
        if range_m:
            birth = range_m.group(1)
            if not death: death = range_m.group(2)
    return (birth or "?", death or "present")


def extract_one_liner(extract: str, title: str) -> str:
    sentences = re.split(r'(?<=[.!?])\s+', extract.strip())
    for sent in sentences[:5]:
        sent = sent.strip()
        if len(sent) > 40 and title.split()[0].lower() in sent.lower():
            return sent
    return sentences[0].strip() if sentences else ""


def extract_anecdote(extract: str, signals: list[str]) -> str:
    all_signals = {**_PRIMARY_SIGNALS, **_SECONDARY_SIGNALS}
    patterns_to_use = []
    for sig in signals:
        patterns_to_use.extend(all_signals.get(sig, []))
    if not patterns_to_use:
        for patterns in _SECONDARY_SIGNALS.values():
            patterns_to_use.extend(patterns)
    sentences = re.split(r'(?<=[.!?])\s+', extract)
    scored = []
    for i, sent in enumerate(sentences):
        if len(sent) < 40: continue
        hits = sum(1 for p in patterns_to_use if re.search(p, sent.lower()))
        pos_bonus = 0.5 if 0.2 < i / max(len(sentences), 1) < 0.85 else 0
        scored.append((hits + pos_bonus, i, sent))
    scored.sort(key=lambda x: (-x[0], x[1]))
    if not scored:
        return " ".join(s for s in sentences[:6] if len(s) > 40)[:900]
    best_idx = scored[0][1]
    start, end = max(0, best_idx - 1), min(len(sentences), best_idx + 3)
    anecdote = " ".join(s for s in sentences[start:end] if len(s.strip()) > 30).strip()
    if len(anecdote) > 900:
        anecdote = anecdote[:900].rsplit(" ", 1)[0] + "…"
    return anecdote


# ─────────────────────────────────────────────────────────────────────────────
# Diversity selection
# ─────────────────────────────────────────────────────────────────────────────

def _estimate_era(extract: str, birth_year: str) -> str:
    try:
        year = int(birth_year)
        if year < 1700: return "ancient-early"
        if year < 1850: return "19th-earlier"
        if year < 1940: return "early-modern"
        return "modern"
    except ValueError:
        return "unknown"


def select_diverse_four(ranked: list[dict]) -> list[dict]:
    if len(ranked) <= 4:
        return ranked
    selected, era_counts = [], {}
    for candidate in ranked:
        era = _estimate_era(candidate["extract"], candidate["birth_year"])
        if era_counts.get(era, 0) >= 3 and len(selected) < 4:
            continue
        selected.append(candidate)
        era_counts[era] = era_counts.get(era, 0) + 1
        if len(selected) == 4: break
    if len(selected) < 4:
        for candidate in ranked:
            if candidate not in selected:
                selected.append(candidate)
            if len(selected) == 4: break
    return selected[:4]


# ─────────────────────────────────────────────────────────────────────────────
# HTML email body
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


def build_email_html(people: list[dict], date_display: str) -> str:
    cards = ""
    for p in people:
        years = f"({p['birth_year']}–{p['death_year']})" if p["birth_year"] != "?" else ""
        tags = "".join(
            f'<span style="display:inline-block;background:#eef7f2;color:#2e6e4e;'
            f'border-radius:5px;font-size:11px;font-weight:600;padding:3px 8px;'
            f'margin:2px 3px 2px 0;letter-spacing:.04em;">'
            f'{SIGNAL_LABELS.get(s, s)}</span>'
            for s in p.get("signals", [])[:4]
        )
        cards += f"""
        <div style="background:#ffffff;border:1px solid #e5e0d8;border-radius:12px;
                    padding:22px 24px 18px;margin-bottom:20px;">
          <div style="margin-bottom:10px;">
            <span style="font-size:18px;font-weight:700;color:#1a1a1a;">{p['name']}</span>
            <span style="font-size:13px;color:#6b7280;margin-left:6px;">{years}</span>
            <p style="font-size:14px;color:#6b7280;font-style:italic;margin:4px 0 0;">{p['tagline']}</p>
          </div>
          <p style="font-size:11px;font-weight:700;color:#2e6e4e;letter-spacing:.1em;
                    text-transform:uppercase;margin:14px 0 6px;">The Anecdote</p>
          <p style="font-size:15px;line-height:1.7;color:#2d2d2d;margin:0;">{p['anecdote']}</p>
          <div style="margin-top:12px;">{tags}</div>
          <a href="{p['url']}" style="display:inline-block;margin-top:14px;font-size:14px;
             color:#2563eb;text-decoration:none;font-weight:500;">Read the full biography &rarr;</a>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#faf9f7;font-family:-apple-system,BlinkMacSystemFont,
             'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
  <div style="max-width:620px;margin:0 auto;padding:24px 16px 40px;">
    <div style="text-align:center;border-bottom:2px solid #e5e0d8;padding-bottom:20px;margin-bottom:28px;">
      <p style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;
                color:#6b7280;margin:0 0 6px;">Daily Digest</p>
      <h1 style="font-size:24px;font-weight:700;color:#2e6e4e;margin:0;line-height:1.25;">
        Wikipedia Biographical Digest</h1>
      <p style="font-size:14px;color:#6b7280;margin:6px 0 0;">
        {date_display} &mdash; Four lives worth knowing about</p>
    </div>
    {cards}
    <p style="text-align:center;font-size:12px;color:#9ca3af;margin-top:28px;
              border-top:1px solid #e5e0d8;padding-top:16px;">
      Generated automatically &bull; Source: Wikipedia
    </p>
  </div>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# Send email
# ─────────────────────────────────────────────────────────────────────────────

def send_email(subject: str, html_body: str) -> None:
    if not SMTP_USER or not SMTP_PASSWORD:
        log.error("Email credentials not set. Check YAHOO_EMAIL and YAHOO_APP_PASSWORD secrets.")
        sys.exit(1)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = RECIPIENT
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    log.info("Connecting to %s:%d…", SMTP_HOST, SMTP_PORT)
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, RECIPIENT, msg.as_string())
    log.info("Email sent to %s", RECIPIENT)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    today        = datetime.date.today()
    month        = today.strftime("%B")
    day          = str(today.day)
    date_display = today.strftime("%-d %B %Y")
    date_str     = today.strftime("%Y-%m-%d")

    log.info("=== Wikipedia Biographical Digest (email) starting for %s ===", date_str)

    candidates = get_on_this_day_people(month, day)
    if not candidates:
        log.error("No candidates found; aborting.")
        sys.exit(1)

    scored_people = []
    for candidate in candidates:
        title = candidate["title"]
        log.info("Fetching biography: %s", title)
        extract = get_page_extract(title)
        if not extract or len(extract) < 300:
            log.info("Skipping %s — biography too short or missing", title)
            continue
        score = score_biography(extract)
        birth_year, death_year = extract_life_years(extract, title)
        scored_people.append({
            "name":       candidate.get("name", title),
            "title":      title,
            "extract":    extract,
            "score":      score,
            "birth_year": birth_year,
            "death_year": death_year,
            "tagline":    extract_one_liner(extract, title),
            "anecdote":   extract_anecdote(extract, score["signals"]),
            "url":        get_page_url(title),
            "signals":    score["signals"],
        })
        time.sleep(0.5)

    if not scored_people:
        log.error("No scoreable biographies found; aborting.")
        sys.exit(1)

    log.info("Scored %d candidate biographies", len(scored_people))
    ranked   = sorted(scored_people, key=lambda x: -x["score"]["total"])
    selected = select_diverse_four(ranked)
    log.info("Selected: %s", [p["name"] for p in selected])

    html_body = build_email_html(selected, date_display)
    subject   = f"Wikipedia Biographical Digest — {date_display}"
    send_email(subject, html_body)
    print(f"Done. Digest emailed for {date_display}.")


if __name__ == "__main__":
    main()
