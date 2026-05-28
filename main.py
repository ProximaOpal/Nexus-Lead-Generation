"""
Nexus OS Lead Generation Backend
FastAPI server — runs on Render.com free tier
Pipelines: Google Maps scraper, Reddit intent miner, PageSpeed scorer, Email extractor
"""

import os
import re
import json
import time
import asyncio
import httpx
import praw
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from bs4 import BeautifulSoup
import urllib.parse

# ─── APP INIT ────────────────────────────────────────────────────────────────

app = FastAPI(title="Nexus OS Lead Gen API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── CONFIG FROM ENV ─────────────────────────────────────────────────────────

REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "script:nexus-os:v1.0")
N8N_WEBHOOK_URL      = os.getenv("N8N_WEBHOOK_URL", "")
APPS_SCRIPT_URL      = os.getenv("APPS_SCRIPT_URL", "")

# ─── IN-MEMORY JOB STORE (persists for session) ───────────────────────────────

jobs: dict = {}          # job_id → { status, leads[], created_at }
lead_cache: list = []    # all captured leads this session

# ─── MODELS ──────────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    keywords: List[str]
    depth: Optional[int] = 1
    city: Optional[str] = "Nairobi"
    niche: Optional[str] = "restaurants"

class Lead(BaseModel):
    name: str
    phone: Optional[str] = ""
    email: Optional[str] = ""
    website: Optional[str] = ""
    city: Optional[str] = ""
    niche: Optional[str] = ""
    score: Optional[int] = None
    status: Optional[str] = "UNKNOWN"
    source: Optional[str] = "google_maps"
    scraped_at: Optional[str] = ""

class WebhookPayload(BaseModel):
    leads: List[dict]
    source: Optional[str] = "render_scraper"

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def extract_emails_from_html(html: str) -> List[str]:
    """Regex email extraction with noise filtering."""
    pattern = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
    found = re.findall(pattern, html)
    blacklist = {"noreply", "example", "wordpress", "sentry", "support@sentry",
                 "no-reply", "donotreply", "test@", "admin@example", "user@example"}
    cleaned = []
    for e in set(found):
        if not any(b in e.lower() for b in blacklist):
            cleaned.append(e.lower())
    return cleaned[:5]  # max 5 emails per site

def make_job_id() -> str:
    return f"job_{int(time.time() * 1000)}"

async def fetch_page_html(url: str) -> str:
    """Fetch raw HTML of a URL for email extraction."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            r = await client.get(url, headers=headers)
            return r.text
    except Exception:
        return ""

async def get_pagespeed_score(url: str) -> Optional[int]:
    """
    Call Google PageSpeed Insights API — free, no billing.
    Returns mobile performance score 0-100. None on failure.
    """
    if not url or not url.startswith("http"):
        return None
    api_url = (
        f"https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
        f"?url={urllib.parse.quote(url)}&strategy=mobile"
    )
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(api_url)
            data = r.json()
            score = data["lighthouseResult"]["categories"]["performance"]["score"]
            return int(score * 100)
    except Exception:
        return None

async def push_to_n8n(leads: list):
    """Fire n8n webhook with new leads for Google Sheets + Gmail outreach."""
    if not N8N_WEBHOOK_URL:
        return
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(N8N_WEBHOOK_URL, json={"leads": leads})
    except Exception:
        pass

async def push_to_apps_script(lead: dict):
    """Write a single lead row to Google Sheets via Apps Script web app."""
    if not APPS_SCRIPT_URL:
        return
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(APPS_SCRIPT_URL, json=lead)
    except Exception:
        pass

# ─── PIPELINE 1: GOOGLE MAPS SCRAPER ─────────────────────────────────────────
# Uses the Google Maps JSON endpoint (no key needed for basic place search)
# and enriches results with email extraction + PageSpeed scoring.

async def scrape_google_maps_places(keyword: str, city: str) -> List[dict]:
    """
    Queries Google Maps via the text search API.
    Falls back to a direct HTTP search scrape if needed.
    Returns list of business dicts.
    """
    results = []
    query = f"{keyword} in {city}"
    encoded = urllib.parse.quote(query)

    # Method: Google Places text search (no billing for basic fields)
    places_url = (
        f"https://maps.googleapis.com/maps/api/place/textsearch/json"
        f"?query={encoded}&key=AIzaSyD-9tSrke72PouQMnMX-a7eZSW0jkFmBWY"
    )

    # We also try the free nominatim + overpass combo for truly zero-cost
    # Primary: scrape from SerpAPI-free Google Maps JSON endpoint
    serpapi_url = (
        f"https://serpapi.com/search.json"
        f"?engine=google_maps&q={encoded}&type=search"
    )

    # Real zero-cost approach: parse Google Maps HTML via search
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    search_url = f"https://www.google.com/maps/search/{encoded}"

    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(search_url, headers=headers)
            html = r.text

            # Extract JSON data blobs embedded in Maps HTML
            # Google Maps embeds business data as JSON in script tags
            pattern = r'\["([^"]+)",null,\[null,null,(-?\d+\.\d+),(-?\d+\.\d+)\]'
            matches = re.findall(pattern, html)

            # Also extract phone numbers
            phone_pattern = r'\+254[\s\-]?\d{3}[\s\-]?\d{3}[\s\-]?\d{3}'
            phones = re.findall(phone_pattern, html)

            # Extract business names from Maps JSON arrays
            name_pattern = r'"([A-Z][^"]{3,50})",\[\d+,\d+\]'
            names = re.findall(name_pattern, html)[:20]

            # Extract website URLs embedded in page
            site_pattern = r'https?://(?!maps\.google|google\.com|goo\.gl)[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}(?:/[^\s"]*)?'
            sites = list(set(re.findall(site_pattern, html)))[:30]

            # Build lead objects from what we scraped
            for i, name in enumerate(names[:15]):
                phone = phones[i] if i < len(phones) else ""
                website = ""
                for s in sites:
                    if any(w.lower() in s.lower() for w in name.lower().split()[:2] if len(w) > 3):
                        website = s
                        break

                results.append({
                    "name": name,
                    "phone": phone,
                    "website": website,
                    "city": city,
                    "niche": keyword,
                    "source": "google_maps",
                    "scraped_at": datetime.utcnow().isoformat()
                })

    except Exception as e:
        print(f"Maps scrape error: {e}")

    return results

async def run_scrape_job(job_id: str, keywords: List[str], city: str, niche: str):
    """Background task: scrape → score → extract emails → push to n8n + Sheets."""
    jobs[job_id]["status"] = "running"
    all_leads = []

    try:
        for keyword in keywords:
            raw = await scrape_google_maps_places(keyword, city)

            for biz in raw:
                lead = dict(biz)

                # Email extraction
                if lead.get("website"):
                    html = await fetch_page_html(lead["website"])
                    emails = extract_emails_from_html(html)
                    lead["email"] = emails[0] if emails else ""

                    # PageSpeed score
                    score = await get_pagespeed_score(lead["website"])
                    lead["score"] = score
                    if score is not None and score < 50:
                        lead["status"] = "BAD SITE"
                    elif score is not None:
                        lead["status"] = "HAS SITE"
                else:
                    lead["email"] = ""
                    lead["score"] = None
                    lead["status"] = "NO SITE"

                lead["niche"] = niche or keyword
                all_leads.append(lead)
                lead_cache.append(lead)

                # Random delay to avoid rate limiting
                await asyncio.sleep(2 + (hash(lead["name"]) % 4))

        jobs[job_id]["status"] = "complete"
        jobs[job_id]["leads"] = all_leads
        jobs[job_id]["count"] = len(all_leads)

        # Push to n8n for Sheets + email outreach
        if all_leads:
            await push_to_n8n(all_leads)

    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)

# ─── PIPELINE 2: REDDIT INTENT MINER ─────────────────────────────────────────

REDDIT_INTENT_KEYWORDS = [
    "need a website",
    "looking for web developer",
    "need web developer",
    "how do I get online",
    "build me a website",
    "website for my business",
    "need someone to make website",
    "affordable website",
    "cheap website",
    "need website kenya",
    "online presence",
    "create website",
]

REDDIT_SUBREDDITS = [
    "Kenya",
    "nairobi",
    "smallbusiness",
    "entrepreneur",
    "Entrepreneur",
    "Freelancer",
    "KenyanMentalHealth",
    "africa",
]

async def mine_reddit_intent() -> List[dict]:
    """
    Mine Reddit for businesses expressing website needs.
    Uses PRAW (official Reddit API) — free tier, 100 req/min.
    Falls back to old.reddit.com JSON if PRAW creds missing.
    """
    leads = []

    # Method 1: PRAW if credentials available
    if REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET:
        try:
            reddit = praw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent=REDDIT_USER_AGENT,
            )
            for sub_name in REDDIT_SUBREDDITS[:4]:
                try:
                    subreddit = reddit.subreddit(sub_name)
                    for keyword in REDDIT_INTENT_KEYWORDS[:5]:
                        for post in subreddit.search(keyword, sort="new", limit=10, time_filter="week"):
                            # Extract any contact info from post text
                            emails = extract_emails_from_html(post.selftext or "")
                            phones = re.findall(r'\+?254[\s\-]?\d{3}[\s\-]?\d{3}[\s\-]?\d{3}', post.selftext or "")

                            leads.append({
                                "name": post.author.name if post.author else "u/unknown",
                                "phone": phones[0] if phones else "",
                                "email": emails[0] if emails else "",
                                "website": f"https://reddit.com{post.permalink}",
                                "city": "Unknown",
                                "niche": "Reddit Intent",
                                "score": None,
                                "status": "INBOUND",
                                "source": "reddit",
                                "intent_text": post.title[:200],
                                "subreddit": sub_name,
                                "upvotes": post.score,
                                "scraped_at": datetime.utcnow().isoformat(),
                            })
                    await asyncio.sleep(1)
                except Exception:
                    continue
        except Exception as e:
            print(f"PRAW error, falling back: {e}")

    # Method 2: old.reddit.com JSON — no key needed
    if not leads:
        headers = {
            "User-Agent": "NexusOS/1.0 lead-gen research bot",
            "Accept": "application/json",
        }
        for sub_name in ["Kenya", "smallbusiness", "entrepreneur"][:2]:
            for kw in REDDIT_INTENT_KEYWORDS[:3]:
                url = (
                    f"https://old.reddit.com/r/{sub_name}/search.json"
                    f"?q={urllib.parse.quote(kw)}&sort=new&limit=10&restrict_sr=1&t=week"
                )
                try:
                    async with httpx.AsyncClient(timeout=15) as client:
                        r = await client.get(url, headers=headers)
                        data = r.json()
                        posts = data.get("data", {}).get("children", [])
                        for p in posts:
                            pd = p.get("data", {})
                            text = (pd.get("selftext") or "") + " " + (pd.get("title") or "")
                            emails = extract_emails_from_html(text)
                            phones = re.findall(r'\+?254[\s\-]?\d{3}[\s\-]?\d{3}[\s\-]?\d{3}', text)
                            leads.append({
                                "name": pd.get("author", "u/unknown"),
                                "phone": phones[0] if phones else "",
                                "email": emails[0] if emails else "",
                                "website": f"https://reddit.com{pd.get('permalink', '')}",
                                "city": "Unknown",
                                "niche": "Reddit Intent",
                                "score": None,
                                "status": "INBOUND",
                                "source": "reddit",
                                "intent_text": pd.get("title", "")[:200],
                                "subreddit": sub_name,
                                "upvotes": pd.get("score", 0),
                                "scraped_at": datetime.utcnow().isoformat(),
                            })
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"Reddit fallback error: {e}")
                    continue

    return leads

# ─── PIPELINE 3: DIRECTORY SCRAPER ───────────────────────────────────────────

KENYA_DIRECTORIES = [
    "https://www.yellowpages.co.ke/search?q={niche}&l={city}",
    "https://www.cylex.co.ke/search/{niche}/{city}.html",
    "https://kenya.businessdirectory.co.ke/search?keyword={niche}",
]

async def scrape_directory(niche: str, city: str) -> List[dict]:
    """Scrape Kenyan business directories for leads."""
    leads = []
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Accept-Language": "en-US,en;q=0.9",
    }

    for template in KENYA_DIRECTORIES[:2]:
        url = template.format(
            niche=urllib.parse.quote(niche),
            city=urllib.parse.quote(city)
        )
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                r = await client.get(url, headers=headers)
                soup = BeautifulSoup(r.text, "html.parser")

                # Generic selectors that work across most directory sites
                # Business name patterns
                name_tags = (
                    soup.find_all("h2", class_=re.compile(r"business|company|listing|title", re.I)) or
                    soup.find_all("h3", class_=re.compile(r"business|company|listing|title", re.I)) or
                    soup.find_all(class_=re.compile(r"business[-_]name|company[-_]name|listing[-_]title", re.I))
                )

                phone_tags = soup.find_all(class_=re.compile(r"phone|tel|contact", re.I))
                email_tags = soup.find_all("a", href=re.compile(r"^mailto:", re.I))
                website_tags = soup.find_all("a", href=re.compile(r"^https?://(?!yellowpages|cylex|businessdirectory)", re.I))

                for i, tag in enumerate(name_tags[:20]):
                    name = tag.get_text(strip=True)
                    if len(name) < 3 or len(name) > 80:
                        continue

                    phone = phone_tags[i].get_text(strip=True) if i < len(phone_tags) else ""
                    email = ""
                    if i < len(email_tags):
                        email = email_tags[i]["href"].replace("mailto:", "").strip()
                    website = ""
                    if i < len(website_tags):
                        href = website_tags[i].get("href", "")
                        if href.startswith("http"):
                            website = href

                    leads.append({
                        "name": name,
                        "phone": phone,
                        "email": email,
                        "website": website,
                        "city": city,
                        "niche": niche,
                        "score": None,
                        "status": "NO SITE" if not website else "HAS SITE",
                        "source": "directory",
                        "scraped_at": datetime.utcnow().isoformat(),
                    })

        except Exception as e:
            print(f"Directory scrape error {url}: {e}")

        await asyncio.sleep(3)

    return leads

# ─── ROUTES ──────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "service": "Nexus OS Lead Gen API",
        "version": "1.0.0",
        "status": "online",
        "pipelines": ["google_maps", "reddit", "directory", "pagespeed", "email"],
        "jobs_active": len([j for j in jobs.values() if j["status"] == "running"]),
        "leads_captured": len(lead_cache),
    }

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

# ── Pipeline 1: Trigger Google Maps scrape ────────────────────────────────────

@app.post("/api/scrape")
async def trigger_scrape(req: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Trigger a Google Maps scrape job.
    Returns job_id immediately. Poll /api/jobs/{job_id} for results.
    """
    job_id = make_job_id()
    jobs[job_id] = {
        "status": "queued",
        "leads": [],
        "count": 0,
        "city": req.city,
        "niche": req.niche,
        "keywords": req.keywords,
        "created_at": datetime.utcnow().isoformat(),
    }
    background_tasks.add_task(
        run_scrape_job, job_id, req.keywords, req.city, req.niche
    )
    return {"job_id": job_id, "status": "queued", "message": "Scrape job started"}

@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Poll job status + results."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs[job_id]

@app.get("/api/jobs")
async def list_jobs():
    """List all jobs this session."""
    return {
        "jobs": [
            {"id": jid, "status": j["status"], "count": j.get("count", 0), "created_at": j.get("created_at")}
            for jid, j in jobs.items()
        ]
    }

# ── Pipeline 2: Reddit intent mining ─────────────────────────────────────────

@app.get("/api/reddit")
async def get_reddit_leads():
    """
    Mine Reddit for intent signals — businesses asking for web dev help.
    Returns posts from r/Kenya, r/smallbusiness, r/entrepreneur.
    """
    leads = await mine_reddit_intent()
    # Also push hot leads to n8n
    inbound = [l for l in leads if l.get("email")]
    if inbound:
        await push_to_n8n(inbound)
    return {"leads": leads, "count": len(leads)}

# ── Pipeline 3: Directory scraper ─────────────────────────────────────────────

@app.post("/api/directory")
async def scrape_directories(niche: str = "restaurants", city: str = "Nairobi"):
    """Scrape Kenyan business directories for the given niche + city."""
    leads = await scrape_directory(niche, city)
    return {"leads": leads, "count": len(leads)}

# ── Pipeline 4: PageSpeed scorer ─────────────────────────────────────────────

@app.get("/api/pagespeed")
async def score_site(url: str):
    """
    Score a single website using Google PageSpeed Insights (free, no key).
    Returns mobile performance score 0-100.
    """
    score = await get_pagespeed_score(url)
    if score is None:
        return {"url": url, "score": None, "status": "error", "label": "UNREACHABLE"}
    label = "NO SITE" if score == 0 else ("BAD SITE" if score < 50 else "HAS SITE")
    return {
        "url": url,
        "score": score,
        "label": label,
        "is_hot_lead": score < 50,
    }

# ── Pipeline 5: Email extractor ───────────────────────────────────────────────

@app.get("/api/extract-email")
async def extract_email(url: str):
    """Visit a URL and extract all contact emails from the page."""
    html = await fetch_page_html(url)
    emails = extract_emails_from_html(html)
    # Also check /contact and /about pages
    for path in ["/contact", "/about", "/contact-us"]:
        try:
            base = url.rstrip("/")
            extra_html = await fetch_page_html(base + path)
            emails.extend(extract_emails_from_html(extra_html))
        except Exception:
            pass
    emails = list(set(emails))[:5]
    return {"url": url, "emails": emails, "count": len(emails)}

# ── Lead store ────────────────────────────────────────────────────────────────

@app.get("/api/leads")
async def get_all_leads():
    """Return all leads captured this session."""
    return {
        "leads": lead_cache,
        "count": len(lead_cache),
        "stats": {
            "no_site":  sum(1 for l in lead_cache if l.get("status") == "NO SITE"),
            "bad_site": sum(1 for l in lead_cache if l.get("status") == "BAD SITE"),
            "emails":   sum(1 for l in lead_cache if l.get("email")),
            "inbound":  sum(1 for l in lead_cache if l.get("source") == "reddit"),
        }
    }

@app.post("/api/leads")
async def add_lead(lead: Lead, background_tasks: BackgroundTasks):
    """Manually add a lead + push to Google Sheets."""
    d = lead.dict()
    d["scraped_at"] = datetime.utcnow().isoformat()
    lead_cache.append(d)
    background_tasks.add_task(push_to_apps_script, d)
    return {"status": "added", "lead": d}

@app.delete("/api/leads")
async def clear_leads():
    lead_cache.clear()
    return {"status": "cleared"}

# ── n8n webhook receiver (n8n calls back here with enriched data) ──────────────

@app.post("/api/webhook/n8n")
async def receive_n8n_data(payload: WebhookPayload):
    """
    Receive enriched leads back from n8n after Sheets write + email send.
    Updates local cache with email-sent status.
    """
    for lead in payload.leads:
        lead["email_sent"] = True
        lead_cache.append(lead)
    return {"status": "received", "count": len(payload.leads)}

# ── Stats ─────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats():
    return {
        "total_leads": len(lead_cache),
        "no_site":  sum(1 for l in lead_cache if l.get("status") == "NO SITE"),
        "bad_site": sum(1 for l in lead_cache if l.get("status") == "BAD SITE"),
        "has_site": sum(1 for l in lead_cache if l.get("status") == "HAS SITE"),
        "inbound":  sum(1 for l in lead_cache if l.get("source") == "reddit"),
        "emails_found": sum(1 for l in lead_cache if l.get("email")),
        "jobs_run": len(jobs),
        "jobs_active": len([j for j in jobs.values() if j["status"] == "running"]),
    }
