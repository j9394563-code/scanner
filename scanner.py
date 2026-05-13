#!/usr/bin/env python3
"""
HantaWatch Live Scanner
=======================
Scans RSS feeds + news sources for new hantavirus cases, suspects, and evacuation flights.
Outputs live-scan.json — consumed by nexura.health via /api/live-scan endpoint.

Run modes:
  python scanner.py              # continuous loop (Oracle Cloud / PythonAnywhere always-on)
  python scanner.py --once       # single scan then exit (GitHub Actions / cron)
  python scanner.py --interval 300  # custom interval in seconds

Free hosting (choose one):
  ── GitHub Actions (recommended — free, reliable) ──────────────────────────────
    1. Put this file in a GitHub repo (public or private)
    2. Copy .github/workflows/scanner.yml to your repo
    3. In repo Settings → Secrets → add:
         GIST_ID   = <ID of a GitHub Gist you created for live-scan.json>
    4. GitHub Actions runs every 15 min automatically — no server needed

  ── PythonAnywhere (free tier) ─────────────────────────────────────────────────
    1. Upload scanner.py to /home/<username>/
    2. In "Tasks" tab create a scheduled task:
         python3 /home/<username>/scanner.py --once
       (free tier: once daily; paid: more frequent)
    3. Or use always-on task: python3 /home/<username>/scanner.py

  ── Oracle Cloud Always Free ────────────────────────────────────────────────────
    1. Create free VM (Ampere A1 ARM — 4 OCPU, 24 GB RAM)
    2. ssh in, install deps, then:
         nohup python3 scanner.py > scanner.log 2>&1 &
    3. Or add to crontab: */15 * * * * python3 /home/ubuntu/scanner.py --once

Requirements:
  pip install feedparser requests beautifulsoup4 lxml
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import feedparser
import requests

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# ─── Configuration ─────────────────────────────────────────────────────────────

SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '900'))   # default: 15 min
OUTPUT_FILE   = os.getenv('OUTPUT_FILE', 'live-scan.json')
MAX_ITEMS     = 500
TIMEOUT       = 20

# GitHub Gist — create one at gist.github.com, paste the ID below or set as env var
# The gist file "live-scan.json" becomes a public URL the website can fetch
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN', '')
GIST_ID      = os.getenv('GIST_ID', '')        # e.g. "abc123def456..."

# Optional: push directly to your Railway server (add a POST endpoint there)
RAILWAY_PUSH_URL = os.getenv('RAILWAY_PUSH_URL', '')   # https://hantavirus.up.railway.app/api/scanner-push
RAILWAY_PUSH_KEY = os.getenv('RAILWAY_PUSH_KEY', '')   # shared secret key

# ─── RSS Feed List ─────────────────────────────────────────────────────────────

RSS_FEEDS = [
    # Google News — English
    'https://news.google.com/rss/search?q=hantavirus+2026&hl=en&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=hantavirus+outbreak+2026&hl=en&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22MV+Hondius%22+hantavirus&hl=en&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=hantavirus+flight+evacuation+2026&hl=en&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=hantavirus+case+death+2026&hl=en&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=hantavirus+cruise+ship+2026&hl=en&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=hantavirus+suspect+quarantine+2026&hl=en&gl=GB&ceid=GB:en',
    'https://news.google.com/rss/search?q=hantavirus+Tenerife+OR+Rotterdam+OR+%22Cape+Verde%22+2026&hl=en&gl=US&ceid=US:en',
    # Google News — German
    'https://news.google.com/rss/search?q=hantavirus+2026&hl=de&gl=DE&ceid=DE:de',
    'https://news.google.com/rss/search?q=hantavirus+Flug+verdacht+2026&hl=de&gl=DE&ceid=DE:de',
    'https://news.google.com/rss/search?q=%22MV+Hondius%22&hl=de&gl=DE&ceid=DE:de',
    'https://news.google.com/rss/search?q=hantavirus+tod+gestorben+2026&hl=de&gl=DE&ceid=DE:de',
    # Official health sources
    'https://www.who.int/feeds/entity/csr/don/en/rss.xml',
    'https://promedmail.org/feed/',
    'https://outbreaknewstoday.com/feed/',
    'https://reliefweb.int/updates/rss.xml?search=hantavirus',
    'https://www.ecdc.europa.eu/en/rss.xml',
    # Other languages
    'https://news.google.com/rss/search?q=hantavirus+cas+2026&hl=fr&gl=FR&ceid=FR:fr',
    'https://news.google.com/rss/search?q=hantavirus+caso+2026&hl=es&gl=AR&ceid=AR:es',
    'https://news.google.com/rss/search?q=hantavirus+caso+2026&hl=pt-BR&gl=BR&ceid=BR:pt-419',
]

# ─── Detection Patterns ────────────────────────────────────────────────────────

RELEVANCE_RE = re.compile(
    r'hantavirus|hanta[- ]virus|mv\s*hondius|hantavir',
    re.IGNORECASE
)

STATUS_PATTERNS = {
    'deceased':    re.compile(r'\b(died?|death|fatal|deceased|killed|gestorben|verstorben|todesfall|tot\b|sterben)', re.I),
    'confirmed':   re.compile(r'\b(confirmed|tested.positive|lab.confirmed|bestätigt|positiv.getestet|laborbestätigt|nachgewiesen)', re.I),
    'symptomatic': re.compile(r'\b(hospitali[sz]ed|icu|intensive.care|ill\b|sick\b|hospitalisiert|erkrankt|symptom|intensivstation)', re.I),
    'recovered':   re.compile(r'\b(recovered|genesen|discharged|entlassen|geheilt)', re.I),
    'suspected':   re.compile(r'\b(suspect|verdacht|possible.case|under.observation|quarantine|isolation)', re.I),
}

# Flight number: BA1234, LH 456, KL1234 etc.
FLIGHT_RE = re.compile(r'\b([A-Z]{2}[0-9]{2,4})\b')

# Not real flight codes (common abbreviations)
FLIGHT_EXCLUDE = {
    'WHO', 'CDC', 'PCR', 'DNA', 'RNA', 'ICU', 'RKI', 'ONT', 'RSS', 'GPS',
    'MRI', 'CAT', 'ECG', 'UK2', 'EU2', 'US2', 'UK4', 'US4',
}

# Country name → (lat, lng, display_name)
GEO_MAP = {
    'netherlands':      (52.13,   5.29,  'Netherlands'),
    'dutch':            (52.13,   5.29,  'Netherlands'),
    'niederlande':      (52.13,   5.29,  'Netherlands'),
    'germany':          (51.17,  10.45,  'Germany'),
    'german':           (51.17,  10.45,  'Germany'),
    'deutschland':      (51.17,  10.45,  'Germany'),
    'uk':               (55.38,  -3.44,  'United Kingdom'),
    'britain':          (55.38,  -3.44,  'United Kingdom'),
    'england':          (51.50,  -0.12,  'United Kingdom'),
    'großbritannien':   (55.38,  -3.44,  'United Kingdom'),
    'france':           (46.23,   2.21,  'France'),
    'frankreich':       (46.23,   2.21,  'France'),
    'spain':            (40.46,  -3.75,  'Spain'),
    'spanien':          (40.46,  -3.75,  'Spain'),
    'tenerife':         (28.29, -16.63,  'Tenerife'),
    'teneriffa':        (28.29, -16.63,  'Tenerife'),
    'argentina':        (-38.42,-63.62,  'Argentina'),
    'argentinien':      (-38.42,-63.62,  'Argentina'),
    'chile':            (-35.68,-71.54,  'Chile'),
    'south africa':     (-30.56,  22.94, 'South Africa'),
    'südafrika':        (-30.56,  22.94, 'South Africa'),
    'usa':              (37.09, -95.71,  'USA'),
    'united states':    (37.09, -95.71,  'USA'),
    'australia':        (-25.27, 133.78, 'Australia'),
    'australien':       (-25.27, 133.78, 'Australia'),
    'switzerland':      (46.82,   8.23,  'Switzerland'),
    'schweiz':          (46.82,   8.23,  'Switzerland'),
    'belgium':          (50.50,   4.47,  'Belgium'),
    'belgien':          (50.50,   4.47,  'Belgium'),
    'portugal':         (39.40,  -8.22,  'Portugal'),
    'norway':           (60.47,   8.47,  'Norway'),
    'norwegen':         (60.47,   8.47,  'Norway'),
    'sweden':           (60.13,  18.64,  'Sweden'),
    'schweden':         (60.13,  18.64,  'Sweden'),
    'singapore':        (1.35,  103.82,  'Singapore'),
    'israel':           (31.05,  34.85,  'Israel'),
    'canada':           (56.13,-106.35,  'Canada'),
    'kanada':           (56.13,-106.35,  'Canada'),
    'cape verde':       (14.93, -23.51,  'Cape Verde'),
    'kapverden':        (14.93, -23.51,  'Cape Verde'),
    'ascension':        (-7.96, -14.37,  'Ascension Island'),
    'st. helena':       (-15.96, -5.73,  'St. Helena'),
    'saint helena':     (-15.96, -5.73,  'St. Helena'),
    'tristan da cunha': (-37.07,-12.31,  'Tristan da Cunha'),
    'rotterdam':        (51.92,   4.48,  'Rotterdam'),
    'amsterdam':        (52.37,   4.90,  'Amsterdam'),
    'south georgia':    (-54.28, -36.49, 'South Georgia'),
    'ushuaia':          (-54.81, -68.31, 'Ushuaia'),
}

# ─── Core Helpers ─────────────────────────────────────────────────────────────

def make_id(text: str) -> str:
    return 'scan_' + hashlib.md5(text.encode('utf-8', errors='ignore')).hexdigest()[:12]


def extract_flights(text: str) -> list[str]:
    """Extract valid IATA-style flight codes from text."""
    raw = FLIGHT_RE.findall(text)
    return [m for m in dict.fromkeys(raw) if m not in FLIGHT_EXCLUDE]


def extract_geo(text: str) -> dict | None:
    """Return first matched location from text."""
    lower = text.lower()
    for keyword, (lat, lng, name) in GEO_MAP.items():
        if keyword in lower:
            return {'country': name, 'lat': lat, 'lng': lng}
    return None


def detect_status(text: str) -> str:
    for status, pattern in STATUS_PATTERNS.items():
        if pattern.search(text):
            return status
    return 'suspected'


def is_relevant(title: str, summary: str) -> bool:
    return bool(RELEVANCE_RE.search(f'{title} {summary}'))


# ─── Feed Fetching ─────────────────────────────────────────────────────────────

def fetch_feed(url: str) -> list[dict]:
    try:
        feed = feedparser.parse(url, request_headers={
            'User-Agent': 'HantaWatch/1.0 (+https://nexura.health)',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
        })
        items = []
        for entry in feed.entries[:25]:
            items.append({
                'title':     (entry.get('title',   '') or '').strip(),
                'link':      entry.get('link',     '') or entry.get('guid', ''),
                'published': entry.get('published','') or entry.get('updated', ''),
                'summary':   (entry.get('summary', '') or entry.get('description', '') or '').strip()[:800],
                'source':    getattr(feed.feed, 'title', urlparse(url).netloc),
            })
        return items
    except Exception as e:
        print(f'  [WARN] {urlparse(url).netloc}: {e}')
        return []


def scan_all_feeds() -> list[dict]:
    all_items = []
    for url in RSS_FEEDS:
        items = fetch_feed(url)
        if items:
            print(f'  {len(items):3d} ← {urlparse(url).netloc}')
        all_items.extend(items)
    return all_items


# ─── Processing ────────────────────────────────────────────────────────────────

def process_items(raw: list[dict], seen_ids: set) -> list[dict]:
    new_items = []
    for item in raw:
        title   = item.get('title',   '')
        summary = item.get('summary', '')
        link    = item.get('link',    '')

        if not is_relevant(title, summary):
            continue

        item_id = make_id(link or title)
        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)

        combined = f'{title} {summary}'
        new_items.append({
            'id':         item_id,
            'title':      title,
            'link':       link,
            'published':  item.get('published', ''),
            'source':     item.get('source',    ''),
            'summary':    summary[:400],
            'status':     detect_status(combined),
            'flights':    extract_flights(combined),
            'geo':        extract_geo(combined),
            'scanned_at': datetime.now(timezone.utc).isoformat(),
        })
    return new_items


# ─── Persistence & Push ────────────────────────────────────────────────────────

def load_existing(path: Path) -> tuple[list, set]:
    if not path.exists():
        return [], set()
    try:
        data  = json.loads(path.read_text(encoding='utf-8'))
        items = data.get('items', [])
        seen  = {i['id'] for i in items if 'id' in i}
        return items, seen
    except Exception:
        return [], set()


def save_output(path: Path, items: list) -> None:
    out = {
        'scanned_at': datetime.now(timezone.utc).isoformat(),
        'total':      len(items),
        'items':      items[:MAX_ITEMS],
    }
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'  Saved {len(out["items"])} items → {path}')
    return out


def push_gist(data: dict) -> None:
    if not GITHUB_TOKEN or not GIST_ID:
        return
    try:
        r = requests.patch(
            f'https://api.github.com/gists/{GIST_ID}',
            headers={
                'Authorization': f'token {GITHUB_TOKEN}',
                'Accept': 'application/vnd.github+json',
            },
            json={'files': {'live-scan.json': {'content': json.dumps(data, ensure_ascii=False)}}},
            timeout=TIMEOUT,
        )
        print(f'  Gist push → HTTP {r.status_code}')
    except Exception as e:
        print(f'  [WARN] Gist push failed: {e}')


def push_railway(data: dict) -> None:
    if not RAILWAY_PUSH_URL or not RAILWAY_PUSH_KEY:
        return
    try:
        r = requests.post(
            RAILWAY_PUSH_URL,
            json=data,
            headers={
                'X-Scanner-Key': RAILWAY_PUSH_KEY,
                'Content-Type':  'application/json',
            },
            timeout=TIMEOUT,
        )
        print(f'  Railway push → HTTP {r.status_code}')
    except Exception as e:
        print(f'  [WARN] Railway push failed: {e}')


# ─── Main scan cycle ───────────────────────────────────────────────────────────

def run_once(output_path: Path, seen_ids: set, existing: list) -> tuple[list, dict]:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'\n[{ts}] Scanning {len(RSS_FEEDS)} feeds…')

    raw      = scan_all_feeds()
    new      = process_items(raw, seen_ids)
    all_items = new + existing

    print(f'  New: {len(new)} | Total: {len(all_items)}')
    out = save_output(output_path, all_items)
    push_gist(out)
    push_railway(out)
    return all_items, out


def main() -> None:
    parser = argparse.ArgumentParser(description='HantaWatch Live Scanner')
    parser.add_argument('--once',     action='store_true', help='Single scan then exit')
    parser.add_argument('--interval', type=int, default=SCAN_INTERVAL, help='Seconds between scans')
    args = parser.parse_args()

    output_path     = Path(OUTPUT_FILE)
    existing, seen  = load_existing(output_path)
    print(f'HantaWatch Scanner — {len(existing)} items loaded')
    print(f'Gist: {"configured" if GIST_ID else "not configured"}')

    if args.once:
        run_once(output_path, seen, existing)
        return

    print(f'Running every {args.interval}s — Ctrl+C to stop')
    while True:
        try:
            existing, _ = run_once(output_path, seen, existing)
        except KeyboardInterrupt:
            print('\nStopped.')
            break
        except Exception as e:
            print(f'[ERROR] {e}')
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
