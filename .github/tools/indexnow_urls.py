#!/usr/bin/env python3
"""Collect URLs to notify via IndexNow after a deploy.

URLs = pages that are new compared to the live sitemap (new or scheduled posts)
     + posts whose content files changed in this push.

Usage: indexnow_urls.py <site_url> <public_dir> [changed_file ...]
Prints one URL per line.
"""
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
LANG_SITEMAPS = ["ko/sitemap.xml", "en/sitemap.xml"]
POST_FILE = re.compile(r"^content/posts/([^/]+)\.(ko|en)\.md$")


def locs(xml_text):
    return {e.text.strip() for e in ET.fromstring(xml_text).findall(".//sm:loc", NS)}


def live_urls(site):
    urls = set()
    for path in LANG_SITEMAPS:
        try:
            with urllib.request.urlopen(f"{site}/{path}", timeout=20) as r:
                urls |= locs(r.read())
        except Exception as e:  # first deploy or site down: treat everything as known
            print(f"[warn] could not fetch live {path}: {e}", file=sys.stderr)
            return None
    return urls


def main():
    site, public, changed = sys.argv[1].rstrip("/"), Path(sys.argv[2]), sys.argv[3:]
    built = set()
    for path in LANG_SITEMAPS:
        built |= locs((public / path).read_text())

    out = set()
    live = live_urls(site)
    if live is not None:
        out |= built - live

    for f in changed:
        m = POST_FILE.match(f)
        if m:
            slug, lang = m.groups()
            url = f"{site}/posts/{slug}/" if lang == "ko" else f"{site}/en/posts/{slug}/"
            if url in built:  # skip drafts / deleted posts
                out.add(url)

    for url in sorted(out):
        print(url)


if __name__ == "__main__":
    main()
