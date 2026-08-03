import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

_WHITESPACE_RE = re.compile(r"\s+")


def parse_html(html_content):
    return BeautifulSoup(html_content, "html.parser")


def extract_title(soup):
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return None


def extract_text(soup):
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    raw_text = soup.get_text(separator=" ")
    return _WHITESPACE_RE.sub(" ", raw_text).strip()


def extract_links(soup, base_url, allowed_domains):
    """Yield normalized, in-scope absolute URLs found on the page."""
    for link_tag in soup.find_all("a", href=True):
        absolute_url = urljoin(base_url, link_tag["href"])
        normalized_url = absolute_url.split("#")[0]
        parsed_url = urlparse(normalized_url)

        if parsed_url.scheme in ("http", "https") and parsed_url.netloc in allowed_domains:
            yield normalized_url
