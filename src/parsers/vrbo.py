"""Vrbo scraper. Vrbo (an Expedia Group brand) is guarded by DataDome, so every request
goes through the Apify Unblocker proxy. The queries below are the compact GraphQL documents
the vrbo.com web app uses, trimmed to the fields the Actor exposes."""

from __future__ import annotations

import asyncio
import json
import re
import uuid
from typing import Any

from apify import Actor

from .http_client import RequestClient

CLIENT_INFO = "shopping-pwa,700f4fd620e6467e6454e383436cc5881ca384b4,us-east-1"
CONTEXT = {"siteId": 9001001, "eapid": 1, "tpid": 9001, "locale": "en_US", "currency": "USD"}

HEADERS = {
    "client-info": CLIENT_INFO,
    "content-type": "application/json",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "x-page-id": "page.Hotel-Search,H,20",
    "x-enable-apq": "true",
    "x-shopping-product-line": "lodging",
    "x-product-line": "lodging",
    "x-parent-brand-id": "vrbo",
    "origin": "https://www.vrbo.com",
    "referer": "https://www.vrbo.com/search",
}

_TEXT = "__typename ... on EGDSStylizedText { text } ... on EGDSPlainText { text }"

SEARCH_Q = """
query VrboSearch($context: ContextInput!, $criteria: PropertySearchCriteriaInput!) {
  propertySearch(context: $context, criteria: $criteria) {
    summary { matchedPropertiesSize }
    pagination { paginationLabel subSets { nextSubSet { size startingIndex } } }
    propertySearchListings {
      __typename
      ... on LodgingCard {
        id
        headingSection {
          heading
          messages { %(T)s }
          featuredMessages { text }
        }
        summarySections {
          __typename
          ... on LodgingCardProductSummarySection {
            messages { %(T)s }
            reviewSummary {
              graphic { ... on EGDSBadge { text accessibility } }
              title { shoppingProductTitle { ... on EGDSStylizedText { text } } }
              subtexts { __typename ... on ShoppingProductContentStylizedTexts { shoppingProductTitle { ... on EGDSStylizedText { text } } } }
            }
          }
        }
        priceSection {
          priceSummary {
            displayMessages {
              lineItems {
                __typename
                ... on DisplayPrice { role price { formatted accessibilityLabel } }
                ... on LodgingEnrichedMessage { value state }
              }
            }
          }
        }
        mediaSection { gallery { media { media { __typename ... on Image { url } } } } }
        cardLink { resource { value } }
      }
    }
  }
}
""" % {"T": _TEXT}

RATES_Q = """
query VrboRates($context: ContextInput!, $eid: ID!, $dateRange: DateRangeInput) {
  propertyRatesDateSelector(context: $context, dateRange: $dateRange, eid: $eid) {
    configuration { currentDate { day month year } beginDate { day month year } endDate { day month year } maxAvailableDays }
    days { date { day month year } displayPrice available checkinValidity checkoutValidity stayConstraints { minimumStayInDays maximumStayInDays } }
  }
}
"""

_DETAIL_SEC = ("sections { header { text } bodySubSections { header { text } elementsV2 { elements { __typename "
               "... on PropertyContent { header { text } items { __typename "
               "... on PropertyContentItemMarkup { content { text } } "
               "... on PropertyContentItemText { content { primary { value } secondary { value } } } } } } } } }")

DETAIL_Q = """
query VrboDetail($context: ContextInput!, $propertyId: String!, $pi: ProductIdentifierInput!) {
  propertyInfo(context: $context, propertyId: $propertyId) {
    id
    summary {
      location { coordinates { latitude longitude } address { addressLine city province countryCode postalCode } }
      spaceOverview { infoItems { ... on PropertyInfoItem { text } } }
      amenities { amenities { header { text } contents { header { text } infoItems { ... on PropertyInfoItem { text } } } } }
    }
    propertyContentSectionGroups {
      aboutThisProperty { %(S)s }
      aboutThisHost { %(S)s }
      policies { %(S)s }
    }
  }
  productHeadline(productIdentifier: $pi, context: $context) { primary secondary seoStructuredData }
  productRatingSummary(productIdentifier: $pi, context: $context) {
    summary { primary secondary accessibilityLabel }
  }
}
""" % {"S": _DETAIL_SEC}


def context(duaid: str | None = None) -> dict:
    return {
        **CONTEXT,
        "device": {"type": "DESKTOP"},
        "identity": {"duaid": duaid or str(uuid.uuid4()), "authState": "ANONYMOUS"},
        "privacyTrackingState": "CAN_NOT_TRACK",
        "debugContext": {"abacusOverrides": []},
    }


def _date_obj(iso: str) -> dict:
    y, m, d = (int(x) for x in iso.split("-"))
    return {"year": y, "month": m, "day": d}


def criteria(*, region_id=None, region_name=None, checkin=None, checkout=None, adults=2,
             start=0, size=50, bounds=None, sort="RECOMMENDED") -> dict:
    date_range = None
    if checkin and checkout:
        date_range = {"checkInDate": _date_obj(checkin), "checkOutDate": _date_obj(checkout)}
    dest = {"regionName": region_name, "regionId": region_id, "coordinates": None, "mapBounds": None}
    if bounds:  # (sw_lat, sw_lng, ne_lat, ne_lng)
        dest["mapBounds"] = [{"latitude": bounds[0], "longitude": bounds[1]},
                             {"latitude": bounds[2], "longitude": bounds[3]}]
        dest["coordinates"] = {"latitude": (bounds[0] + bounds[2]) / 2, "longitude": (bounds[1] + bounds[3]) / 2}
    return {
        "primary": {"dateRange": date_range, "destination": dest, "rooms": [{"adults": adults, "children": []}]},
        "secondary": {
            "counts": [{"id": "resultsStartingIndex", "value": start}, {"id": "resultsSize", "value": size}],
            "booleans": [], "selections": [{"id": "sort", "value": sort}], "ranges": [],
        },
    }


class VrboScraper:
    def __init__(self, client: RequestClient) -> None:
        self.client = client
        self._duaid = str(uuid.uuid4())

    async def _gql(self, operation: str, query: str, variables: dict, ok_path: str) -> Any | None:
        body = [{"operationName": operation, "query": query, "variables": variables}]

        def ok(status, text):
            if status != 200:
                return False
            return ok_path in text or '"errors"' in text

        res = await self.client.request("POST", "https://www.vrbo.com/graphql",
                                        headers=HEADERS, json_body=body, timeout=50, ok_predicate=ok)
        if not res.ok:
            return None
        try:
            payload = res.json()
        except Exception:
            return None
        item = payload[0] if isinstance(payload, list) else payload
        if not isinstance(item, dict):
            return None
        if item.get("errors") and not item.get("data"):
            Actor.log.debug(f"[vrbo] {operation} errors: {json.dumps(item['errors'])[:200]}")
            return None
        return item.get("data")

    async def typeahead(self, query: str) -> dict | None:
        url = f"https://www.vrbo.com/api/v4/typeahead/{query}"
        params = {"client": "SearchForm", "format": "json", "lob": "HOTELS",
                  "maxresults": "5", "siteid": "9001001", "locale": "en_US", "dest": "true"}
        res = await self.client.request("GET", url, params=params, timeout=40,
                                        headers={"accept": "application/json", "accept-language": "en-US"})
        if not res.ok:
            return None
        try:
            data = res.json()
        except Exception:
            return None
        for sr in data.get("sr") or []:
            if sr.get("gaiaId"):
                names = sr.get("regionNames") or {}
                return {"regionId": sr["gaiaId"], "regionName": names.get("fullName"),
                        "type": sr.get("type"), "coordinates": sr.get("coordinates")}
        return None

    async def search(self, *, region_id=None, region_name=None, bounds=None, checkin=None,
                     checkout=None, adults=2, max_listings=500, sort="RECOMMENDED"):
        """Yield unique Vrbo listing rows for a market."""
        seen: set[str] = set()
        start = 0
        page_size = 50
        matched = None
        while len(seen) < max_listings:
            crit = criteria(region_id=region_id, region_name=region_name, bounds=bounds,
                            checkin=checkin, checkout=checkout, adults=adults, start=start, size=page_size, sort=sort)
            data = await self._gql("VrboSearch", SEARCH_Q, {"context": context(self._duaid), "criteria": crit},
                                   "propertySearchListings")
            if not data:
                break
            ps = data.get("propertySearch") or {}
            matched = (ps.get("summary") or {}).get("matchedPropertiesSize", matched)
            cards = [c for c in (ps.get("propertySearchListings") or []) if c.get("__typename") == "LodgingCard"]
            if not cards:
                break
            new_in_page = 0
            for card in cards:
                row = parse_search_card(card)
                if row and row["id"] not in seen:
                    seen.add(row["id"])
                    new_in_page += 1
                    yield row
                    if len(seen) >= max_listings:
                        return
            pagination = ps.get("pagination") or {}
            nxt = ((pagination.get("subSets") or {}).get("nextSubSet") or {})
            next_index = nxt.get("startingIndex")
            if not next_index or (matched and start + page_size >= matched) or new_in_page == 0:
                break
            start = next_index

    async def rates(self, eid: str, date_from: str, date_to: str) -> dict | None:
        variables = {"context": context(self._duaid), "eid": str(eid),
                     "dateRange": {"start": _date_obj(date_from), "end": _date_obj(date_to)}}
        data = await self._gql("VrboRates", RATES_Q, variables, "propertyRatesDateSelector")
        if not data:
            return None
        return parse_rates(data.get("propertyRatesDateSelector") or {})

    async def rates_batch(self, eids: list[str], date_from: str, date_to: str) -> dict[str, dict]:
        """Fetch nightly rate calendars for several properties in one aliased request."""
        if not eids:
            return {}
        parts = []
        for i, eid in enumerate(eids):
            parts.append(
                f'p{i}: propertyRatesDateSelector(context: $context, dateRange: $dateRange, eid: "{eid}") '
                "{ days { date { day month year } displayPrice available checkinValidity "
                "stayConstraints { minimumStayInDays } } }"
            )
        query = ("query VrboRatesMulti($context: ContextInput!, $dateRange: DateRangeInput) { "
                 + " ".join(parts) + " }")
        variables = {"context": context(self._duaid),
                     "dateRange": {"start": _date_obj(date_from), "end": _date_obj(date_to)}}
        data = await self._gql("VrboRatesMulti", query, variables, "propertyRatesDateSelector")
        out: dict[str, dict] = {}
        if not data:
            return out
        for i, eid in enumerate(eids):
            sel = data.get(f"p{i}")
            if sel:
                out[str(eid)] = parse_rates(sel)
        return out

    async def detail(self, property_id: str) -> dict | None:
        variables = {"context": context(self._duaid), "propertyId": str(property_id),
                     "pi": {"id": str(property_id), "type": "PROPERTY_ID"}}
        data = await self._gql("VrboDetail", DETAIL_Q, variables, "propertyInfo")
        if not data:
            return None
        return parse_detail(data, property_id)

    async def resolve_listing_number(self, listing_number: str) -> str | None:
        """Resolve a public Vrbo listing number (e.g. from a /1234567 URL) to the internal
        Expedia property id used by the API."""
        url = f"https://www.vrbo.com/{listing_number}"
        res = await self.client.request("GET", url, timeout=90,
                                        ok_predicate=lambda s, t: s == 200 and "itemProp" in t)
        if not res.ok:
            return None
        m = re.search(r'itemProp="identifier"\s+content="(\d+)"', res.text)
        return m.group(1) if m else None


# ---- parsers -------------------------------------------------------------------------------
def _texts(nodes) -> list[str]:
    out = []
    for n in nodes or []:
        t = n.get("text")
        if t:
            out.append(t)
    return out


def parse_search_card(card: dict) -> dict | None:
    lid = card.get("id")
    if not lid:
        return None
    heading = card.get("headingSection") or {}
    summary_sections = card.get("summarySections") or []
    review = None
    room_msgs = []
    for sec in summary_sections:
        if sec.get("reviewSummary"):
            review = sec["reviewSummary"]
        room_msgs.extend(_texts(sec.get("messages")))
    rating = None
    reviews_count = None
    rating_label = None
    if review:
        graphic = review.get("graphic") or {}
        rating = _to_float(graphic.get("text"))
        rating_label = (review.get("title") or {}).get("shoppingProductTitle", {}).get("text")
        for sub in review.get("subtexts") or []:
            title = (sub.get("shoppingProductTitle") or {}).get("text")
            if title and "review" in title.lower():
                reviews_count = _first_int(title)

    price_lead = None
    price_strike = None
    price_secondary = None
    avg_nightly = None
    for grp in ((card.get("priceSection") or {}).get("priceSummary") or {}).get("displayMessages") or []:
        for li in grp.get("lineItems") or []:
            if li.get("__typename") == "DisplayPrice":
                formatted = (li.get("price") or {}).get("formatted")
                if li.get("role") == "LEAD":
                    price_lead = formatted
                elif li.get("role") == "STRIKEOUT":
                    price_strike = formatted
            elif li.get("__typename") == "LodgingEnrichedMessage":
                state = li.get("state") or ""
                if "AVERAGE_NIGHTLY" in state:
                    avg_nightly = li.get("value")
                elif "SECONDARY_PRICE" in state or "NUMBER_OF_NIGHTS" in state:
                    price_secondary = li.get("value")

    images = []
    for m in ((card.get("mediaSection") or {}).get("gallery") or {}).get("media") or []:
        url = (m.get("media") or {}).get("url")
        if url:
            images.append(url)
    link = ((card.get("cardLink") or {}).get("resource") or {}).get("value")
    clean_url = None
    if link:
        clean_url = link.split("?")[0]

    heading_msgs = _texts(heading.get("messages"))
    return {
        "platform": "vrbo",
        "id": str(lid),
        "url": clean_url or f"https://www.vrbo.com/{lid}",
        "name": heading.get("heading"),
        "propertyType": heading_msgs[0] if heading_msgs else None,
        "roomInfo": heading_msgs,
        "neighborhood": (heading.get("featuredMessages") or [{}])[0].get("text") if heading.get("featuredMessages") else None,
        "rating": rating,
        "ratingLabel": rating_label,
        "reviewsCount": reviews_count,
        "priceLabel": price_lead,
        "originalPriceLabel": price_strike,
        "priceQualifier": price_secondary,
        "avgNightlyLabel": avg_nightly,
        "images": images[:12],
        "thumbnail": images[0] if images else None,
    }


def parse_rates(node: dict) -> dict:
    cfg = node.get("configuration") or {}
    days = []
    for d in node.get("days") or []:
        date = d.get("date") or {}
        iso = f"{date.get('year'):04d}-{date.get('month'):02d}-{date.get('day'):02d}" if date.get("year") else None
        days.append({
            "date": iso,
            "available": bool(d.get("available")),
            "price": d.get("displayPrice") or None,
            "minNights": (d.get("stayConstraints") or {}).get("minimumStayInDays"),
            "checkinValidity": d.get("checkinValidity"),
        })
    return {"maxAvailableDays": cfg.get("maxAvailableDays"), "days": days}


def parse_detail(data: dict, property_id: str) -> dict:
    info = data.get("propertyInfo") or {}
    summary = info.get("summary") or {}
    loc = summary.get("location") or {}
    coord = loc.get("coordinates") or {}
    addr = loc.get("address") or {}
    space = [i.get("text") for i in (summary.get("spaceOverview") or {}).get("infoItems") or [] if i.get("text")]

    amenities = []
    amenity_groups = {}
    for grp in (summary.get("amenities") or {}).get("amenities") or []:
        for content in grp.get("contents") or []:
            cat = (content.get("header") or {}).get("text") or "General"
            items = [it.get("text") for it in content.get("infoItems") or [] if it.get("text")]
            amenity_groups.setdefault(cat, []).extend(items)
            amenities.extend(items)

    headline = data.get("productHeadline") or {}
    seo = {}
    if headline.get("seoStructuredData"):
        try:
            seo = json.loads(headline["seoStructuredData"])
        except Exception:
            seo = {}
    agg = seo.get("aggregateRating") or {}
    rating_summary = (data.get("productRatingSummary") or {}).get("summary") or {}

    groups = info.get("propertyContentSectionGroups") or {}
    description = _flatten_sections(groups.get("aboutThisProperty"))
    host_text = _flatten_sections(groups.get("aboutThisHost"))
    policies = _flatten_sections(groups.get("policies"))

    return {
        "platform": "vrbo",
        "id": str(property_id),
        "url": f"https://www.vrbo.com/{property_id}",
        "name": headline.get("primary"),
        "headline": headline.get("secondary"),
        "coordinates": {"lat": coord.get("latitude"), "lng": coord.get("longitude")},
        "address": {
            "full": addr.get("addressLine"), "city": addr.get("city"),
            "province": addr.get("province"), "countryCode": addr.get("countryCode"),
            "postalCode": addr.get("postalCode") or None,
        },
        "roomInfo": space,
        "rating": _to_float(agg.get("ratingValue")) or _first_float(rating_summary.get("primary")),
        "ratingLabel": rating_summary.get("secondary"),
        "reviewsCount": _to_int(agg.get("reviewCount")),
        "amenities": amenities,
        "amenitiesByCategory": amenity_groups or None,
        "amenitiesCount": len(amenities),
        "description": description,
        "hostInfo": host_text,
        "houseRules": policies,
        "images": [seo.get("image")] if seo.get("image") else [],
        "thumbnail": seo.get("image"),
    }


def _flatten_sections(group) -> str | None:
    if not group:
        return None
    parts: list[str] = []
    for section in group.get("sections") or []:
        for sub in section.get("bodySubSections") or []:
            for eg in sub.get("elementsV2") or []:
                for el in eg.get("elements") or []:
                    header = (el.get("header") or {}).get("text")
                    if header:
                        parts.append(header)
                    for item in el.get("items") or []:
                        content = item.get("content") or {}
                        if isinstance(content.get("text"), str):
                            parts.append(_strip_html(content["text"]))
                        primary = (content.get("primary") or {}).get("value")
                        if primary:
                            parts.append(primary)
    text = "\n".join(p for p in parts if p)
    return text or None


def _strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", " ", s).replace("&amp;", "&").strip()


def _to_float(v):
    try:
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _to_int(v):
    try:
        return int(str(v).replace(",", "").split(".")[0])
    except (TypeError, ValueError):
        return None


def _first_int(s):
    m = re.search(r"[0-9][0-9,]*", s or "")
    return int(m.group(0).replace(",", "")) if m else None


def _first_float(s):
    m = re.search(r"[0-9]+[.,]?[0-9]*", s or "")
    return _to_float(m.group(0)) if m else None