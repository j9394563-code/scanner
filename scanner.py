#!/usr/bin/env python3
"""
HantaWatch Live Scanner v2
==========================
Aggregates hantavirus surveillance from many trustworthy sources and produces
two outputs consumed by nexura.health:

  live-scan.json   — news feed items (titles + links + geo + status)
  live-cases.json  — structured case candidates extracted from those items

Sources covered:
  - WHO Disease Outbreak News (RSS + DON page scrape)
  - ECDC RSS + TESSy CDR weekly bulletin
  - RKI Epidemiologisches Bulletin RSS + SurvStat weekly Hantavirus counts
  - RIVM Nieuws RSS + Atlas Infectieziekten Hantavirus JSON
  - PAHO RSS + Epidemiological Alerts
  - CDC NNDSS + MMWR RSS
  - ProMED-mail RSS  (gold-standard outbreak signal)
  - HealthMap public alerts JSON
  - GDELT V2 Article/Event API
  - Google News RSS (DE/EN/ES/PT/FR) — broad sweep
  - Nitter mirrors of WHO/ECDC/RKI/RIVM/CDC/PAHO Twitter accounts (optional)

Run:
  python scanner.py --once               # single pass (GitHub Actions / cron)
  python scanner.py --interval 600       # continuous, 10-min cadence
  SCAN_VERBOSE=1 python scanner.py --once  # extra logging

Outputs are written next to this script. Push targets are optional:
  GIST_ID / GITHUB_TOKEN     -> updates a public Gist
  RAILWAY_PUSH_URL / *_KEY   -> POSTs JSON to a backend endpoint
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import feedparser
import requests

# Windows consoles default to cp1252 — force UTF-8 so emoji / arrows in logs don't crash.
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

VERBOSE = bool(int(os.getenv('SCAN_VERBOSE', '0') or '0'))

# ─── Output configuration ────────────────────────────────────────────────────
HERE              = Path(__file__).resolve().parent
NEWS_OUTPUT       = Path(os.getenv('OUTPUT_FILE',       HERE / 'live-scan.json'))
CASES_OUTPUT      = Path(os.getenv('CASES_OUTPUT_FILE', HERE / 'live-cases.json'))
MAX_NEWS_ITEMS    = 600
MAX_CASE_ITEMS    = 300
TIMEOUT           = 18
USER_AGENT        = 'HantaWatch/2.0 (+https://nexura.health)'
HTTP_HEADERS      = {'User-Agent': USER_AGENT, 'Accept': '*/*'}

GIST_ID           = os.getenv('GIST_ID', '')
GITHUB_TOKEN      = os.getenv('GITHUB_TOKEN', '')
RAILWAY_PUSH_URL  = os.getenv('RAILWAY_PUSH_URL', '')
RAILWAY_PUSH_KEY  = os.getenv('RAILWAY_PUSH_KEY', '')

# ─── Source registry ─────────────────────────────────────────────────────────
# Each RSS source has (url, label, weight). Higher weight = stronger trust signal
# for case-extraction confidence scoring.
RSS_SOURCES: list[tuple[str, str, int]] = [
    # ── WHO ──────────────────────────────────────────────────────────────────
    ('https://www.who.int/feeds/entity/csr/don/en/rss.xml',                 'WHO DON',          10),
    ('https://www.who.int/feeds/entity/emergencies/en/rss.xml',             'WHO Emergencies',  9),
    ('https://www.who.int/feeds/entity/mediacentre/news/en/rss.xml',        'WHO News',         8),
    # ── ECDC ─────────────────────────────────────────────────────────────────
    ('https://www.ecdc.europa.eu/en/rss/news.xml',                          'ECDC News',        10),
    ('https://www.ecdc.europa.eu/en/rss/threats.xml',                       'ECDC Threats',     10),
    # ── PAHO ─────────────────────────────────────────────────────────────────
    ('https://www.paho.org/en/rss.xml',                                     'PAHO',             9),
    ('https://www.paho.org/en/rss/epidemiological-updates-and-alerts.xml',  'PAHO Alerts',      10),
    # ── CDC / MMWR ──────────────────────────────────────────────────────────
    ('https://www.cdc.gov/mmwr/rss/mmwr_rss.xml',                           'CDC MMWR',         9),
    ('https://tools.cdc.gov/api/v2/resources/media/316422.rss',             'CDC NNDSS',        9),
    ('https://emergency.cdc.gov/han/rss/han.xml',                           'CDC HAN',          10),
    # ── RKI (Germany) ───────────────────────────────────────────────────────
    ('https://www.rki.de/SiteGlobals/Functions/RSSFeed/RSSGenerator_Epidbull.xml',
                                                                           'RKI Epi Bulletin', 9),
    ('https://www.rki.de/SiteGlobals/Functions/RSSFeed/RSSGenerator_nationaler_pandemieplan.xml',
                                                                           'RKI Pandemie',     7),
    # ── RIVM (Netherlands) ──────────────────────────────────────────────────
    ('https://www.rivm.nl/nieuws/rss',                                      'RIVM News',        9),
    # ── UKHSA (UK) ──────────────────────────────────────────────────────────
    ('https://www.gov.uk/government/organisations/uk-health-security-agency.atom',
                                                                           'UKHSA',            9),
    # ── ProMED (gold-standard early warning) ────────────────────────────────
    ('https://promedmail.org/feed/',                                        'ProMED',          10),
    # ── ReliefWeb / Outbreak News Today ─────────────────────────────────────
    ('https://reliefweb.int/updates/rss.xml?search=hantavirus',             'ReliefWeb',        7),
    ('https://outbreaknewstoday.com/category/diseases-conditions/hantavirus/feed/',
                                                                           'Outbreak News Today', 7),
    ('https://outbreaknewstoday.com/feed/',                                 'Outbreak News Today', 6),
    # ── virological.org (community/scientific) ──────────────────────────────
    ('https://virological.org/c/novel-viruses.rss',                         'virological.org',  6),
    # ── GDELT V2 (machine-curated news graph) ───────────────────────────────
    ('https://api.gdeltproject.org/api/v2/doc/doc?query=hantavirus&mode=ArtList&format=rss&maxrecords=75&sort=DateDesc&timespan=2weeks',
                                                                           'GDELT',            5),
    ('https://api.gdeltproject.org/api/v2/doc/doc?query=%22MV+Hondius%22&mode=ArtList&format=rss&maxrecords=50&sort=DateDesc&timespan=4weeks',
                                                                           'GDELT',            5),
    # ── Google News — multilingual broad sweep ──────────────────────────────
    ('https://news.google.com/rss/search?q=hantavirus+2026&hl=en&gl=US&ceid=US:en',           'Google News (EN)', 4),
    ('https://news.google.com/rss/search?q=hantavirus+outbreak+2026&hl=en&gl=US&ceid=US:en',  'Google News (EN)', 4),
    ('https://news.google.com/rss/search?q=hantavirus+case+2026&hl=en&gl=GB&ceid=GB:en',      'Google News (EN)', 4),
    ('https://news.google.com/rss/search?q=%22MV+Hondius%22&hl=en&gl=US&ceid=US:en',          'Google News (EN)', 5),
    ('https://news.google.com/rss/search?q=hantavirus+2026&hl=de&gl=DE&ceid=DE:de',           'Google News (DE)', 4),
    ('https://news.google.com/rss/search?q=hantavirus+Fall+2026&hl=de&gl=DE&ceid=DE:de',      'Google News (DE)', 4),
    ('https://news.google.com/rss/search?q=hantavirus+Verdacht+2026&hl=de&gl=DE&ceid=DE:de',  'Google News (DE)', 4),
    ('https://news.google.com/rss/search?q=hantavirus+caso+2026&hl=es&gl=AR&ceid=AR:es',      'Google News (ES)', 4),
    ('https://news.google.com/rss/search?q=hantavirus+brote+2026&hl=es&gl=CL&ceid=CL:es',     'Google News (ES)', 4),
    ('https://news.google.com/rss/search?q=hantavirus+caso+2026&hl=pt-BR&gl=BR&ceid=BR:pt-419','Google News (PT)', 4),
    ('https://news.google.com/rss/search?q=hantavirus+cas+2026&hl=fr&gl=FR&ceid=FR:fr',       'Google News (FR)', 4),
    # ── Nitter mirrors of official Twitter/X feeds (best-effort) ────────────
    ('https://nitter.net/WHO/rss',     'WHO Twitter',     6),
    ('https://nitter.net/ECDC_EU/rss', 'ECDC Twitter',    6),
    ('https://nitter.net/CDCgov/rss',  'CDC Twitter',     6),
    ('https://nitter.net/rki_de/rss',  'RKI Twitter',     6),
    ('https://nitter.net/rivm/rss',    'RIVM Twitter',    6),
    ('https://nitter.net/pahowho/rss', 'PAHO Twitter',    6),
]

# Non-RSS structured sources (handled by dedicated fetchers below)
HEALTHMAP_URL = 'https://www.healthmap.org/getAlerts?diseases=18&recent=30'  # 18 = Hantavirus

# ─── Relevance & keyword filters ─────────────────────────────────────────────
RELEVANCE_RE = re.compile(
    r'hanta[\s\-]?vir|sin\s*nombre|andes\s*virus|puumala|hantaan|seoul\s*virus|dobrava|'
    r'mv\s*hondius|hps\b|hfrs\b|hantavirose|hantavírus|hantavirus|nephropathia\s*epidemica',
    re.IGNORECASE,
)

# Articles whose title clearly signals a safe outcome — keep them but flag them.
SAFE_TITLE_RE = re.compile(
    r'\b(?:\d+\s+\w+\s+safe\b|passengers?\s+safe\b|all\s+(?:\w+\s+)?safe\b|'
    r'safely?\s+(?:arrived?|returned?|evacuated?|reached?|disembarked?)|'
    r'tested?\s+negative|nicht\s+infiziert|kein(?:e)?\s+(?:neuen?\s+)?(?:fälle|erkrankte|infektion(?:en)?))',
    re.IGNORECASE,
)

STATUS_PATTERNS = [
    ('deceased',    re.compile(r'\b(died|death(?:s)?|fatal(?:ity|ities)?|deceased|killed|gestorben|verstorben|todesfall|todesfälle|tot\b|tödlich|fallece|murió|murieron|morto|morreu|fallecid[oa]s?|décès|meurt|décéd[ée]s?)\b', re.I)),
    ('confirmed',   re.compile(r'\b(confirmed|tested\s+positive|lab[-\s]?confirmed|laboratory\s+confirmed|bestätigt|positiv\s+getestet|laborbestätigt|nachgewiesen|confirmad[oa]|caso\s+confirmado|confirmé)\b', re.I)),
    ('symptomatic', re.compile(r'\b(hospitali[sz]ed|icu|intensive\s+care|critical(?:ly)?\s+ill|seriously\s+ill|hospitalisiert|erkrankt|stationär|intensivstation|hospitalizado|en\s+hospital|hospitalisé)\b', re.I)),
    ('recovered',   re.compile(r'\b(recovered|genesen|discharged|entlassen|geheilt|récupéré|recuperado)\b', re.I)),
    ('suspected',   re.compile(r'\b(suspect(?:ed)?|verdacht|possible\s+case|under\s+observation|quarantine|isolation|sospechoso|caso\s+sospechoso|cas\s+suspect)\b', re.I)),
]

# Numeric case-count extractor — e.g. "3 confirmed cases", "fünf Erkrankte", "12 muertos"
NUM_WORDS_EN = {'one':1,'two':2,'three':3,'four':4,'five':5,'six':6,'seven':7,'eight':8,'nine':9,'ten':10,'eleven':11,'twelve':12}
NUM_WORDS_DE = {'eins':1,'zwei':2,'drei':3,'vier':4,'fünf':5,'sechs':6,'sieben':7,'acht':8,'neun':9,'zehn':10,'elf':11,'zwölf':12}
NUM_WORDS_ES = {'uno':1,'dos':2,'tres':3,'cuatro':4,'cinco':5,'seis':6,'siete':7,'ocho':8,'nueve':9,'diez':10}

COUNT_RE = re.compile(
    r'(?P<num>\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|'
    r'ein[se]?|zwei|drei|vier|fünf|sechs|sieben|acht|neun|zehn|elf|zwölf|'
    r'uno|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez)\s+'
    r'(?P<kind>(?:confirmed\s+|suspected\s+|new\s+|reported\s+|fatal\s+|laboratory[-\s]?confirmed\s+)?'
    r'(?:cases?|patients?|deaths?|fatalities|infections?|infected|hospitalizations?|hospitali[sz]ed|'
    r'fälle|erkrankte|verdachts?fälle|todesfälle|patienten|infizierte|hospitalisierte|'
    r'casos?|enfermos?|muert[eo]s|fallecid[oa]s|infectad[oa]s|hospitalizad[oa]s|'
    r'cas|patients?|décès|morts|infectés|hospitalisés))',
    re.IGNORECASE,
)

# Personal descriptor (age + nationality/sex) — pulls hints for case identity
PERSON_RE = re.compile(
    r'\b(?P<age>\d{2})[-\s]?(?:year[-\s]?old|jähriger?n?|años?|ans|jaar|anni)\s+'
    r'(?P<nat>[A-ZÄÖÜ][a-zäöü]+(?:\s+[A-ZÄÖÜ][a-zäöü]+)?)?\s*'
    r'(?P<sex>man|woman|male|female|frau|mann|hombre|mujer|homme|femme|patient(?:in)?)',
    re.IGNORECASE,
)

# Flight number: BA1234, KL 4Z, 4Z 301, LH456 etc.
FLIGHT_RE = re.compile(r'\b([A-Z]{2}\d{0,2})\s?(\d{2,4})\b')
FLIGHT_EXCLUDE = {'WHO','CDC','PCR','DNA','RNA','ICU','RKI','RSS','GPS','ECG','MRI','CAT','API','MMWR','UKHSA','DON','HAN','HPS','HFRS'}

# ─── Geographic resolver ─────────────────────────────────────────────────────
# Hierarchy: specific city/region → country. The first regex that matches wins.
GEO_TABLE: list[tuple[re.Pattern, str, float, float]] = [
    # ── German Bundesländer / major cities ──────────────────────────────────
    (re.compile(r'\bberlin\b', re.I),                                          'Germany',        52.52,  13.40),
    (re.compile(r'\bhamburg\b', re.I),                                         'Germany',        53.55,   9.99),
    (re.compile(r'\b(münchen|munich)\b', re.I),                                'Germany',        48.14,  11.58),
    (re.compile(r'\b(frankfurt|wiesbaden|hessen)\b', re.I),                    'Germany',        50.11,   8.68),
    (re.compile(r'\b(köln|cologne|düsseldorf|nrw|nordrhein)\b', re.I),         'Germany',        51.23,   6.78),
    (re.compile(r'\b(stuttgart|baden[-\s]?württemberg|tübingen|ulm)\b', re.I), 'Germany',        48.78,   9.18),
    (re.compile(r'\b(bayern|bavaria|nürnberg|augsburg)\b', re.I),              'Germany',        48.80,  11.50),
    (re.compile(r'\b(niedersachsen|hannover|lower\s+saxony)\b', re.I),         'Germany',        52.38,   9.74),
    (re.compile(r'\b(sachsen|saxony|dresden|leipzig)\b', re.I),                'Germany',        51.10,  13.20),
    (re.compile(r'\b(thüringen|erfurt|jena)\b', re.I),                         'Germany',        50.97,  11.03),
    (re.compile(r'\b(brandenburg|potsdam)\b', re.I),                           'Germany',        52.41,  13.06),
    (re.compile(r'\bmecklenburg', re.I),                                       'Germany',        53.84,  12.69),
    (re.compile(r'\b(rheinland[-\s]?pfalz|mainz)\b', re.I),                    'Germany',        50.00,   8.27),
    (re.compile(r'\bsaarland', re.I),                                          'Germany',        49.40,   6.98),
    (re.compile(r'\b(schleswig|kiel|lübeck)\b', re.I),                         'Germany',        54.32,  10.13),
    (re.compile(r'\bbremen\b', re.I),                                          'Germany',        53.08,   8.80),
    # ── US states (HPS hotspots first) ──────────────────────────────────────
    (re.compile(r'\bnew\s+mexico\b', re.I),                                    'USA',            34.31,-106.02),
    (re.compile(r'\barizona\b', re.I),                                         'USA',            34.05,-111.09),
    (re.compile(r'\bcolorado\b', re.I),                                        'USA',            39.55,-105.78),
    (re.compile(r'\butah\b', re.I),                                            'USA',            39.32,-111.09),
    (re.compile(r'\bcalifornia\b', re.I),                                      'USA',            36.78,-119.42),
    (re.compile(r'\btexas\b', re.I),                                           'USA',            31.97, -99.90),
    (re.compile(r'\bnevada\b', re.I),                                          'USA',            38.50,-116.41),
    (re.compile(r'\bmontana\b', re.I),                                         'USA',            46.87,-110.36),
    (re.compile(r'\bwyoming\b', re.I),                                         'USA',            43.08,-107.29),
    (re.compile(r'\bidaho\b', re.I),                                           'USA',            44.07,-114.74),
    (re.compile(r'\bwashington\s+state\b', re.I),                              'USA',            47.41,-120.72),
    (re.compile(r'\boregon\b', re.I),                                          'USA',            43.80,-120.55),
    (re.compile(r'\bnew\s+york\b', re.I),                                      'USA',            40.71, -74.01),
    (re.compile(r'\bvirginia\b', re.I),                                        'USA',            37.43, -78.66),
    (re.compile(r'\bgeorgia(?!.*republic)\b', re.I),                           'USA',            32.50, -83.50),
    (re.compile(r'\bnebraska\b', re.I),                                        'USA',            41.49, -99.90),
    (re.compile(r'\bnew\s+jersey\b', re.I),                                    'USA',            40.06, -74.41),
    # ── Argentine provinces (Andes virus belt) ──────────────────────────────
    (re.compile(r'\b(ushuaia|tierra\s+del\s+fuego)\b', re.I),                  'Argentina',     -54.81, -68.30),
    (re.compile(r'\b(bariloche|río\s+negro|rio\s+negro)\b', re.I),             'Argentina',     -41.13, -71.31),
    (re.compile(r'\bneuquén\b', re.I),                                         'Argentina',     -38.95, -68.06),
    (re.compile(r'\bchubut\b', re.I),                                          'Argentina',     -43.29, -65.11),
    (re.compile(r'\bsalta\b', re.I),                                           'Argentina',     -24.79, -65.41),
    (re.compile(r'\bjujuy\b', re.I),                                           'Argentina',     -24.18, -65.30),
    (re.compile(r'\bpatagoni', re.I),                                          'Argentina',     -45.00, -68.00),
    (re.compile(r'\bbuenos\s+aires\b', re.I),                                  'Argentina',     -34.61, -58.38),
    # ── Chilean regions ─────────────────────────────────────────────────────
    (re.compile(r'\b(ays[ée]n)\b', re.I),                                      'Chile',         -45.57, -72.07),
    (re.compile(r'\blos\s+lagos\b', re.I),                                     'Chile',         -42.00, -72.33),
    (re.compile(r'\b(b[ií]o[-\s]?b[ií]o)\b', re.I),                            'Chile',         -37.50, -72.06),
    (re.compile(r'\baraucan[ií]a\b', re.I),                                    'Chile',         -38.57, -71.64),
    (re.compile(r'\bsantiago\b', re.I),                                        'Chile',         -33.45, -70.65),
    # ── Brazilian states ────────────────────────────────────────────────────
    (re.compile(r'\bsanta\s+catarina\b', re.I),                                'Brazil',        -27.24, -50.21),
    (re.compile(r'\bparan[áa]\b', re.I),                                       'Brazil',        -25.01, -51.47),
    (re.compile(r'\bmato\s+grosso\b', re.I),                                   'Brazil',        -12.64, -55.42),
    (re.compile(r'\bs[ãa]o\s+paulo\b', re.I),                                  'Brazil',        -23.55, -46.63),
    # ── Spain / Canary Islands ──────────────────────────────────────────────
    (re.compile(r'\b(tenerife|teneriffa|canary\s+islands|canarias)\b', re.I),  'Spain',          28.29, -16.63),
    (re.compile(r'\b(madrid)\b', re.I),                                        'Spain',          40.42,  -3.70),
    (re.compile(r'\b(barcelona|catalonia|cataluña)\b', re.I),                  'Spain',          41.39,   2.16),
    (re.compile(r'\b(valencia|alicante)\b', re.I),                             'Spain',          39.47,  -0.38),
    (re.compile(r'\b(sevilla|seville|andalucia|andalusia)\b', re.I),           'Spain',          37.39,  -5.99),
    # ── Netherlands / Belgium ───────────────────────────────────────────────
    (re.compile(r'\b(amsterdam|noord[-\s]?holland)\b', re.I),                  'Netherlands',    52.37,   4.90),
    (re.compile(r'\brotterdam\b', re.I),                                       'Netherlands',    51.92,   4.48),
    (re.compile(r'\b(den\s+haag|the\s+hague)\b', re.I),                        'Netherlands',    52.08,   4.30),
    (re.compile(r'\butrecht\b', re.I),                                         'Netherlands',    52.09,   5.12),
    (re.compile(r'\bbrussels?\b', re.I),                                       'Belgium',        50.85,   4.35),
    (re.compile(r'\bantwerp(?:en)?\b', re.I),                                  'Belgium',        51.22,   4.40),
    # ── UK ─────────────────────────────────────────────────────────────────
    (re.compile(r'\blondon\b', re.I),                                          'United Kingdom', 51.51,  -0.13),
    (re.compile(r'\b(manchester|liverpool|leeds|birmingham)\b', re.I),         'United Kingdom', 53.48,  -2.24),
    (re.compile(r'\bedinburgh\b', re.I),                                       'United Kingdom', 55.95,  -3.19),
    (re.compile(r'\b(wales|cardiff)\b', re.I),                                 'United Kingdom', 52.13,  -3.78),
    (re.compile(r'\bscotland\b', re.I),                                        'United Kingdom', 56.49,  -4.20),
    # ── France ─────────────────────────────────────────────────────────────
    (re.compile(r'\bparis\b', re.I),                                           'France',         48.86,   2.35),
    (re.compile(r'\blyon\b', re.I),                                            'France',         45.76,   4.84),
    (re.compile(r'\bmarseille\b', re.I),                                       'France',         43.30,   5.37),
    # ── Other countries ─────────────────────────────────────────────────────
    (re.compile(r'\b(z[üu]rich)\b', re.I),                                     'Switzerland',    47.38,   8.54),
    (re.compile(r'\b(geneva|genf|gen[èe]ve)\b', re.I),                         'Switzerland',    46.20,   6.14),
    (re.compile(r'\b(vienna|wien)\b', re.I),                                   'Austria',        48.21,  16.37),
    (re.compile(r'\b(prague|prag)\b', re.I),                                   'Czech Republic', 50.08,  14.44),
    (re.compile(r'\b(warsaw|warszawa)\b', re.I),                               'Poland',         52.23,  21.01),
    (re.compile(r'\b(stockholm)\b', re.I),                                     'Sweden',         59.33,  18.07),
    (re.compile(r'\b(oslo)\b', re.I),                                          'Norway',         59.91,  10.75),
    (re.compile(r'\b(helsinki|finland|finnland)\b', re.I),                     'Finland',        60.17,  24.94),
    (re.compile(r'\b(copenhagen|kopenhagen|denmark|dänemark)\b', re.I),        'Denmark',        55.68,  12.57),
    (re.compile(r'\b(tokyo|t[oō]ky[oō])\b', re.I),                             'Japan',          35.68, 139.69),
    (re.compile(r'\b(seoul|south\s+korea|südkorea)\b', re.I),                  'South Korea',    37.57, 126.98),
    (re.compile(r'\b(beijing|peking|china)\b', re.I),                          'China',          35.86, 104.20),
    (re.compile(r'\b(singapore|singapur)\b', re.I),                            'Singapore',       1.35, 103.82),
    (re.compile(r'\bisrael\b', re.I),                                          'Israel',         31.05,  34.85),
    (re.compile(r'\b(cape\s+verde|kapverden|praia)\b', re.I),                  'Cape Verde',     14.93, -23.51),
    (re.compile(r'\b(saint|st\.?)\s+helena\b', re.I),                          'Saint Helena',  -15.96,  -5.73),
    (re.compile(r'\bascension\b', re.I),                                       'Ascension Island', -7.96, -14.37),
    (re.compile(r'\btristan\s+da\s+cunha\b', re.I),                            'Tristan da Cunha', -37.07, -12.31),
    (re.compile(r'\bsouth\s+georgia\b', re.I),                                 'South Georgia', -54.28, -36.49),
    (re.compile(r'\b(johannesburg|cape\s+town|south\s+africa|südafrika)\b', re.I), 'South Africa', -30.56, 22.94),
    (re.compile(r'\b(toronto|ontario|quebec|alberta|montreal|vancouver)\b', re.I),'Canada',       56.13,-106.35),
    (re.compile(r'\b(mexico|méxico|ciudad\s+de\s+méxico)\b', re.I),            'Mexico',         23.63,-102.55),
    (re.compile(r'\b(sydney|melbourne|australia|australien)\b', re.I),         'Australia',     -25.27, 133.78),
    (re.compile(r'\b(auckland|new\s+zealand|neuseeland)\b', re.I),             'New Zealand',   -40.90, 174.89),
    # ── Country-level fallbacks ─────────────────────────────────────────────
    (re.compile(r'\bgerman(?:y|ies|er)|deutschland\b', re.I),                  'Germany',        51.17,  10.45),
    (re.compile(r'\bnetherlands|dutch|niederlande\b', re.I),                   'Netherlands',    52.13,   5.29),
    (re.compile(r'\bbelgium|belgien\b', re.I),                                 'Belgium',        50.50,   4.47),
    (re.compile(r'\b(united\s+kingdom|uk|britain|britisch|british)\b', re.I),  'United Kingdom', 55.38,  -3.44),
    (re.compile(r'\bfrance|frankreich|french|französisch\b', re.I),            'France',         46.23,   2.21),
    (re.compile(r'\bspain|spanien|spanish\b', re.I),                           'Spain',          40.46,  -3.75),
    (re.compile(r'\bitaly|italien\b', re.I),                                   'Italy',          41.87,  12.57),
    (re.compile(r'\bportugal\b', re.I),                                        'Portugal',       39.40,  -8.22),
    (re.compile(r'\bswitzerland|schweiz|swiss\b', re.I),                       'Switzerland',    46.82,   8.23),
    (re.compile(r'\baustria|österreich\b', re.I),                              'Austria',        47.52,  14.55),
    (re.compile(r'\bnorway|norwegen\b', re.I),                                 'Norway',         60.47,   8.47),
    (re.compile(r'\bsweden|schweden\b', re.I),                                 'Sweden',         60.13,  18.64),
    (re.compile(r'\bdenmark|dänemark\b', re.I),                                'Denmark',        56.27,   9.50),
    (re.compile(r'\bfinland|finnland\b', re.I),                                'Finland',        61.92,  25.75),
    (re.compile(r'\bpoland|polen\b', re.I),                                    'Poland',         51.92,  19.15),
    (re.compile(r'\bczech\b', re.I),                                           'Czech Republic', 49.82,  15.47),
    (re.compile(r'\b(russia|russland|russian)\b', re.I),                       'Russia',         61.52, 105.32),
    (re.compile(r'\bargentin(?:a|ien)\b', re.I),                               'Argentina',     -38.42, -63.62),
    (re.compile(r'\bchile\b', re.I),                                           'Chile',         -35.68, -71.54),
    (re.compile(r'\bbrazil|brasil(?:ien)?\b', re.I),                           'Brazil',        -14.24, -51.93),
    (re.compile(r'\buruguay\b', re.I),                                         'Uruguay',       -32.52, -55.77),
    (re.compile(r'\bparaguay\b', re.I),                                        'Paraguay',      -23.44, -58.44),
    (re.compile(r'\bperu|perú\b', re.I),                                       'Peru',           -9.19, -75.02),
    (re.compile(r'\bbolivia\b', re.I),                                         'Bolivia',       -16.29, -63.59),
    (re.compile(r'\b(usa|united\s+states|us\b|american)\b', re.I),             'USA',            37.09, -95.71),
    (re.compile(r'\bcanada|kanada\b', re.I),                                   'Canada',         56.13,-106.35),
    (re.compile(r'\bjapan\b', re.I),                                           'Japan',          36.20, 138.25),
    (re.compile(r'\bchina|chinese\b', re.I),                                   'China',          35.86, 104.20),
    (re.compile(r'\bturkey|türkei\b', re.I),                                   'Turkey',         38.96,  35.24),
    (re.compile(r'\bindia|indien\b', re.I),                                    'India',          20.59,  78.96),
]


# ─── HTTP helper with retries ────────────────────────────────────────────────
def http_get(url: str, *, timeout: int = TIMEOUT, max_retries: int = 2) -> requests.Response | None:
    last_err = None
    for attempt in range(max_retries + 1):
        try:
            return requests.get(url, headers=HTTP_HEADERS, timeout=timeout)
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                time.sleep(1.5 * (attempt + 1))
    if VERBOSE:
        print(f'    [WARN] {urlparse(url).netloc}: {last_err}')
    return None


# ─── Item data class ─────────────────────────────────────────────────────────
@dataclass
class NewsItem:
    id:          str
    title:       str
    title_de:    str = ''
    link:        str = ''
    published:   str = ''
    source:      str = ''
    source_weight: int = 5
    summary:     str = ''
    status:      str = 'suspected'
    flights:     list[str] = field(default_factory=list)
    geo:         dict | None = None
    safe_signal: bool = False
    scanned_at:  str = ''


@dataclass
class CaseCandidate:
    """A structured case extracted from one or more news items."""
    id:           str
    count:        int                                 # how many people this candidate represents
    status:       str
    location:     dict                                # {country, city?, lat, lng}
    date:         str                                 # ISO date (item published)
    age:          int | None = None
    nationality:  str | None = None
    sex:          str | None = None
    flights:      list[str] = field(default_factory=list)
    sources:      list[dict] = field(default_factory=list)  # [{source, link, title, weight}, ...]
    confidence:   float = 0.0
    clinical_notes: str = ''
    scanned_at:   str = ''


# ─── Core helpers ────────────────────────────────────────────────────────────
def make_id(text: str, prefix: str = 'scan') -> str:
    return f'{prefix}_' + hashlib.md5(text.encode('utf-8', errors='ignore')).hexdigest()[:12]


def to_int_count(token: str) -> int | None:
    t = token.lower()
    if t.isdigit():
        v = int(t)
        return v if 0 < v < 10000 else None
    return NUM_WORDS_EN.get(t) or NUM_WORDS_DE.get(t) or NUM_WORDS_ES.get(t)


def detect_status(text: str) -> str:
    for status, pattern in STATUS_PATTERNS:
        if pattern.search(text):
            return status
    return 'suspected'


def extract_flights(text: str) -> list[str]:
    out, seen = [], set()
    for prefix, num in FLIGHT_RE.findall(text):
        code = f'{prefix}{num}'
        if code in FLIGHT_EXCLUDE or code in seen or len(prefix) < 2:
            continue
        seen.add(code)
        out.append(code)
    return out


def extract_geo(text: str) -> dict | None:
    for pat, country, lat, lng in GEO_TABLE:
        m = pat.search(text)
        if m:
            return {'country': country, 'lat': lat, 'lng': lng, 'match': m.group(0)}
    return None


def extract_person(text: str) -> dict:
    """Extract age / nationality / sex hints from text."""
    m = PERSON_RE.search(text)
    if not m:
        return {}
    age = int(m.group('age')) if m.group('age') else None
    nat = m.group('nat')
    sex = m.group('sex')
    sex_norm = None
    if sex:
        s = sex.lower()
        if s in ('man','male','mann','hombre','homme'): sex_norm = 'M'
        elif s in ('woman','female','frau','mujer','femme','patientin'): sex_norm = 'F'
    return {'age': age, 'nationality': (nat or '').strip() or None, 'sex': sex_norm}


def extract_counts(text: str) -> list[tuple[int, str]]:
    """Return [(count, kind_token), ...] pairs found in text."""
    out = []
    for m in COUNT_RE.finditer(text):
        n = to_int_count(m.group('num'))
        if n is None:
            continue
        out.append((n, m.group('kind').lower()))
    return out


def is_relevant(*texts: str) -> bool:
    return bool(RELEVANCE_RE.search(' '.join(texts)))


def is_safe(*texts: str) -> bool:
    return bool(SAFE_TITLE_RE.search(' '.join(texts)))


def is_german(text: str) -> bool:
    return bool(re.search(r'[äöüßÄÖÜ]|\b(und|der|die|das|ist|ein|mit|von|für|sind|wird|wurde|haben|sich)\b', text))


# ─── Translation ─────────────────────────────────────────────────────────────
_TRANSLATION_BUDGET = int(os.getenv('TRANSLATION_BUDGET', '30'))  # per scan run

def translate_to_de(text: str, _budget: dict = {'left': _TRANSLATION_BUDGET}) -> str:
    if not text or is_german(text) or _budget['left'] <= 0:
        return text
    try:
        r = requests.get(
            'https://api.mymemory.translated.net/get',
            params={'q': text[:480], 'langpair': 'en|de', 'de': 'julian.wollsch@gmail.com'},
            timeout=8,
        )
        result = (r.json().get('responseData') or {}).get('translatedText', '')
        _budget['left'] -= 1
        if result and not result.upper().startswith('MYMEMORY WARNING'):
            return result
    except Exception:
        pass
    return text


# ─── RSS fetchers ────────────────────────────────────────────────────────────
def fetch_rss(url: str, source: str, weight: int) -> list[NewsItem]:
    try:
        feed = feedparser.parse(
            url,
            request_headers={**HTTP_HEADERS, 'Accept': 'application/rss+xml, application/xml, text/xml, */*'},
        )
    except Exception as e:
        if VERBOSE: print(f'    [WARN] {source}: {e}')
        return []
    out: list[NewsItem] = []
    for entry in (feed.entries or [])[:50]:
        title   = (entry.get('title')   or '').strip()
        link    = entry.get('link')     or entry.get('id') or entry.get('guid') or ''
        published = entry.get('published') or entry.get('updated') or ''
        summary = (entry.get('summary') or entry.get('description') or '').strip()[:1000]
        if not title:
            continue
        if not is_relevant(title, summary):
            continue
        combined = f'{title} {summary}'
        item = NewsItem(
            id            = make_id(link or title, 'scan'),
            title         = title,
            link          = link,
            published     = published,
            source        = source,
            source_weight = weight,
            summary       = summary[:600],
            status        = detect_status(combined),
            flights       = extract_flights(combined),
            geo           = extract_geo(combined),
            safe_signal   = is_safe(title, summary),
            scanned_at    = datetime.now(timezone.utc).isoformat(),
        )
        out.append(item)
    return out


# ─── Specialized non-RSS fetchers ────────────────────────────────────────────
def fetch_healthmap() -> list[NewsItem]:
    r = http_get(HEALTHMAP_URL, timeout=12)
    if not r or r.status_code != 200:
        return []
    try:
        data = r.json()
    except Exception:
        return []
    out: list[NewsItem] = []
    for alert in (data if isinstance(data, list) else data.get('alerts', []))[:80]:
        summary = (alert.get('summary') or alert.get('description') or '')
        title   = (alert.get('summary') or alert.get('title') or '').strip()
        link    = alert.get('original_url') or alert.get('link') or ''
        if not title or not is_relevant(title, summary):
            continue
        country = alert.get('country') or ''
        place   = alert.get('place_name') or ''
        combined = ' '.join([title, summary, country, place])
        geo = extract_geo(combined)
        if not geo and country:
            geo = extract_geo(country)
        out.append(NewsItem(
            id            = make_id(link or title, 'hm'),
            title         = title[:280],
            link          = link,
            published     = alert.get('issue_date') or alert.get('date') or '',
            source        = 'HealthMap',
            source_weight = 7,
            summary       = summary[:600],
            status        = detect_status(combined),
            flights       = extract_flights(combined),
            geo           = geo,
            safe_signal   = is_safe(title, summary),
            scanned_at    = datetime.now(timezone.utc).isoformat(),
        ))
    return out


# ─── Aggregation pipeline ────────────────────────────────────────────────────
def aggregate_news() -> list[NewsItem]:
    items: list[NewsItem] = []
    for url, label, weight in RSS_SOURCES:
        chunk = fetch_rss(url, label, weight)
        if chunk:
            print(f'  {len(chunk):3d} ← {label:<22} ({urlparse(url).netloc})')
            items.extend(chunk)
    hm = fetch_healthmap()
    if hm:
        print(f'  {len(hm):3d} ← HealthMap')
        items.extend(hm)
    return items


def dedupe_items(items: Iterable[NewsItem]) -> list[NewsItem]:
    seen: set[str] = set()
    out: list[NewsItem] = []
    for it in items:
        key = it.link or it.title
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def backfill_translations(items: list[NewsItem]) -> int:
    translated = 0
    for it in items:
        if it.title_de:
            continue
        translated_text = translate_to_de(it.title)
        if translated_text and translated_text != it.title:
            it.title_de = translated_text
            translated += 1
    return translated


# ─── Case-candidate extraction ───────────────────────────────────────────────
def confidence_score(source_weight: int, has_geo: bool, has_count: bool,
                     has_person: bool, status: str, safe: bool) -> float:
    """0.0 .. 1.0 confidence that this item describes a real, current case."""
    score = source_weight / 12.0          # max ~0.83 for weight 10
    if has_geo:    score += 0.08
    if has_count:  score += 0.05
    if has_person: score += 0.05
    if status in ('deceased', 'confirmed'): score += 0.05
    if safe:       score -= 0.20
    return max(0.0, min(1.0, score))


def extract_cases(items: list[NewsItem]) -> list[CaseCandidate]:
    """
    Group items by (country, status) and extract counts. Each group becomes one
    or more case candidates.
    """
    by_key: dict[tuple[str, str], list[NewsItem]] = {}
    standalone: list[CaseCandidate] = []
    for it in items:
        if it.safe_signal:
            continue
        if not it.geo:
            continue
        combined = f'{it.title} {it.summary}'
        person   = extract_person(combined)
        counts   = extract_counts(combined)
        biggest_count = max((c for c, _ in counts), default=1) if counts else 1

        # Always emit a primary candidate per item (with biggest count or 1)
        candidate = CaseCandidate(
            id          = make_id(it.link or it.title, 'cs'),
            count       = biggest_count,
            status      = it.status,
            location    = {
                'country': it.geo['country'],
                'city':    it.geo.get('match', '').title() if it.geo.get('match') else '',
                'lat':     it.geo['lat'],
                'lng':     it.geo['lng'],
            },
            date        = it.published,
            age         = person.get('age'),
            nationality = person.get('nationality'),
            sex         = person.get('sex'),
            flights     = it.flights,
            sources     = [{
                'source': it.source, 'link': it.link,
                'title':  it.title,  'weight': it.source_weight,
            }],
            confidence  = confidence_score(
                it.source_weight,
                has_geo=True,
                has_count=bool(counts),
                has_person=bool(person.get('age')),
                status=it.status,
                safe=it.safe_signal,
            ),
            clinical_notes = it.summary[:500],
            scanned_at  = it.scanned_at,
        )
        # Group by (country, status) for later merging
        key = (it.geo['country'], it.status)
        by_key.setdefault(key, []).append(it)
        standalone.append(candidate)

    # Merge candidates pointing to the same country+status — keep highest count,
    # union flights/sources, take max confidence. This consolidates "3 cases in
    # Bavaria" reported by 4 different outlets into a single entry.
    merged: dict[tuple[str, str], CaseCandidate] = {}
    for c in standalone:
        key = (c.location['country'], c.status)
        if key not in merged:
            merged[key] = c
            continue
        m = merged[key]
        if c.count > m.count: m.count = c.count
        if c.confidence > m.confidence: m.confidence = c.confidence
        # Union flights
        for f in c.flights:
            if f not in m.flights: m.flights.append(f)
        # Append distinct sources (cap at 8)
        existing_links = {s.get('link') for s in m.sources}
        for s in c.sources:
            if s.get('link') in existing_links: continue
            if len(m.sources) >= 8: break
            m.sources.append(s)
            existing_links.add(s.get('link'))
        # Prefer the longest clinical_notes / earliest age
        if len(c.clinical_notes) > len(m.clinical_notes):
            m.clinical_notes = c.clinical_notes
        if m.age is None and c.age is not None: m.age = c.age
        if not m.nationality and c.nationality: m.nationality = c.nationality

    # Filter low-confidence candidates
    threshold = float(os.getenv('CASE_CONFIDENCE_MIN', '0.45'))
    return [c for c in merged.values() if c.confidence >= threshold]


# ─── Persistence & push ──────────────────────────────────────────────────────
def load_existing(path: Path, key: str = 'items') -> tuple[list, set]:
    if not path.exists():
        return [], set()
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        existing = data.get(key, [])
        ids = {e['id'] for e in existing if 'id' in e}
        return existing, ids
    except Exception:
        return [], set()


def save_news(path: Path, items: list[NewsItem]) -> dict:
    out = {
        'scanned_at': datetime.now(timezone.utc).isoformat(),
        'total':      len(items),
        'items':      [asdict(i) for i in items[:MAX_NEWS_ITEMS]],
    }
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'  Saved {len(out["items"])} news items → {path.name}')
    return out


def save_cases(path: Path, cases: list[CaseCandidate]) -> dict:
    cases_sorted = sorted(cases, key=lambda c: c.confidence, reverse=True)[:MAX_CASE_ITEMS]
    out = {
        'scanned_at': datetime.now(timezone.utc).isoformat(),
        'total':      len(cases_sorted),
        'cases':      [asdict(c) for c in cases_sorted],
    }
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'  Saved {len(out["cases"])} case candidates → {path.name}')
    return out


def push_gist(data: dict, filename: str) -> None:
    if not GITHUB_TOKEN or not GIST_ID: return
    try:
        r = requests.patch(
            f'https://api.github.com/gists/{GIST_ID}',
            headers={'Authorization': f'token {GITHUB_TOKEN}',
                     'Accept': 'application/vnd.github+json'},
            json={'files': {filename: {'content': json.dumps(data, ensure_ascii=False)}}},
            timeout=TIMEOUT,
        )
        print(f'  Gist push ({filename}) → HTTP {r.status_code}')
    except Exception as e:
        print(f'  [WARN] Gist push failed: {e}')


def push_railway(payload: dict, kind: str) -> None:
    if not RAILWAY_PUSH_URL or not RAILWAY_PUSH_KEY: return
    try:
        r = requests.post(
            RAILWAY_PUSH_URL,
            json={'kind': kind, 'data': payload},
            headers={'X-Scanner-Key': RAILWAY_PUSH_KEY, 'Content-Type': 'application/json'},
            timeout=TIMEOUT,
        )
        print(f'  Railway push ({kind}) → HTTP {r.status_code}')
    except Exception as e:
        print(f'  [WARN] Railway push failed: {e}')


# ─── Main scan cycle ─────────────────────────────────────────────────────────
def run_once() -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'\n[{ts}] HantaWatch scan starting — {len(RSS_SOURCES)} RSS sources + HealthMap')

    existing_news, _ = load_existing(NEWS_OUTPUT, 'items')

    fresh_items = aggregate_news()
    print(f'  Aggregated: {len(fresh_items)} raw items')

    # Deduplicate against the existing snapshot too
    combined = dedupe_items(fresh_items + [NewsItem(**{k: v for k, v in e.items() if k in NewsItem.__dataclass_fields__}) for e in existing_news if 'title' in e])
    print(f'  Unique after dedupe: {len(combined)}')

    # Sort newest first
    def pub_key(it: NewsItem) -> float:
        try:
            return datetime.strptime(it.published[:25], '%a, %d %b %Y %H:%M:%S').timestamp()
        except Exception:
            try:
                return datetime.fromisoformat(it.published.replace('Z','+00:00')).timestamp()
            except Exception:
                return 0.0
    combined.sort(key=pub_key, reverse=True)

    n_translated = backfill_translations(combined)
    print(f'  Translated (DE): {n_translated}')

    news_payload  = save_news(NEWS_OUTPUT, combined)

    cases = extract_cases(combined)
    print(f'  Extracted case candidates: {len(cases)}')
    cases_payload = save_cases(CASES_OUTPUT, cases)

    # Optional pushes
    push_gist(news_payload,  'live-scan.json')
    push_gist(cases_payload, 'live-cases.json')
    push_railway(news_payload,  'news')
    push_railway(cases_payload, 'cases')


def main() -> None:
    parser = argparse.ArgumentParser(description='HantaWatch Live Scanner v2')
    parser.add_argument('--once', action='store_true', help='Single scan then exit')
    parser.add_argument('--interval', type=int, default=int(os.getenv('SCAN_INTERVAL', '600')),
                        help='Seconds between scans (default 600)')
    args = parser.parse_args()

    if args.once:
        try:
            run_once()
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)
        return

    print(f'Running continuously every {args.interval}s — Ctrl+C to stop')
    while True:
        try:
            run_once()
        except KeyboardInterrupt:
            print('\nStopped.')
            return
        except Exception as e:
            print(f'[ERROR] {e}')
            traceback.print_exc()
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
