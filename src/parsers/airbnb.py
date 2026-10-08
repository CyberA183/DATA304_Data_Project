"""Airbnb scraper: market search with recursive map-tile subdivision, listing detail,
availability calendar and dated pricing. Talks to Airbnb's internal GraphQL (the same
persisted queries the website uses) over an Apify residential proxy."""

from __future__ import annotations

import asyncio
import base64
import json
import re
from typing import Any

from apify import Actor

from .http_client import RequestClient

API_KEY = "d306zoyjsyarp7ifhu67rjxn52tv0t20"

# Persisted-query hashes used as defaults. They are refreshed automatically from the
# website's JS bundle if Airbnb rotates them (see _resolve_hash).
DEFAULT_HASHES = {
    "StaysSearch": "aa52154ae19d9c581fa59773a72c719f4844df441b9253fdb46f5f3da5b836ed",
    "PdpAvailabilityCalendar": "8f08e03c7bd16fcad3c92a3592c19a8b559a0d0855a84028d1163d4733ed9ade",
    "StaysPdpBookItQuery": "dbb612ce6e09072ae8f2e9364a2b07d63d18f43b01128cc4d62d0c1e3a916fc8",
}

TREATMENT_FLAGS = [
    "feed_map_decouple_m11_treatment", "recommended_amenities_2024_treatment_b",
    "filter_redesign_2024_treatment", "filter_reordering_2024_roomtype_treatment",
    "p2_category_bar_removal_treatment", "selected_filters_2024_treatment",
    "recommended_filters_2024_treatment_b", "m13_search_input_phase2_treatment",
    "m13_search_input_services_enabled", "m13_2025_experiences_p2_treatment",
    "homes_p25_refresh_2025_treatment",
]

BASE_HEADERS = {
    "x-airbnb-api-key": API_KEY,
    "content-type": "application/json",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "x-airbnb-graphql-platform": "web",
    "x-airbnb-graphql-platform-client": "minimalist-niobe",
    "x-csrf-without-token": "1",
}

_ROOM_ID_RE = re.compile(r"/rooms/(?:plus/)?(\d+)")
_HASH_RE_TMPL = r"name:'{op}',type:'query',operationId:'([a-f0-9]{{64}})'"
_RATING_RE = re.compile(r"([0-9]+[.,][0-9]+)\D+([0-9][0-9,]*)")


def _decode_listing_id(encoded: str | None) -> str | None:
    if not encoded:
        return None
    try:
        decoded = base64.b64decode(encoded).decode()
        return decoded.split(":")[-1]
    except Exception:
        return None


def listing_id_from_url(url: str) -> str | None:
    m = _ROOM_ID_RE.search(url)
    return m.group(1) if m else None


class AirbnbScraper:
    def __init__(self, client: RequestClient, currency: str = "USD", locale: str = "en",
                 hash_cache: dict | None = None) -> None:
        self.client = client
        self.currency = currency
        self.locale = locale
        self.hashes = dict(DEFAULT_HASHES)
        if hash_cache:
            self.hashes.update({k: v for k, v in hash_cache.items() if v})
        self._hash_lock = asyncio.Lock()

    # ---- persisted-query hash self-healing -------------------------------------------------
    async def _resolve_hash(self, operation: str) -> str | None:
        """Fetch the current operationId for a GraphQL operation from Airbnb's JS bundles."""
        async with self._hash_lock:
            pages = {
                "StaysSearch": "https://www.airbnb.com/s/homes",
            }.get(operation, "https://www.airbnb.com/rooms/48145872")
            res = await self.client.request("GET", pages, headers={
                "accept-language": "en-US,en;q=0.9",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
            })
            if not res.ok:
                return None
            bundles = re.findall(r'https://a0\.muscache\.com/airbnb/static/packages/web/[^"\s]+\.js', res.text)
            pat = re.compile(_HASH_RE_TMPL.format(op=operation))
            for url in dict.fromkeys(bundles):
                jb = await self.client.request("GET", url, headers={"accept-language": "en-US"})
                if not jb.ok:
                    continue
                m = pat.search(jb.text)
                if m:
                    self.hashes[operation] = m.group(1)
                    Actor.log.info(f"[airbnb] refreshed hash for {operation}")
                    return m.group(1)
        return None

    async def _gql(self, operation: str, variables: dict, method: str = "POST",
                   ok_key: str | None = "data") -> Any | None:
        """Call a persisted GraphQL query, refreshing the hash once if it is rejected."""
        for refresh in (False, True):
            if refresh:
                if not await self._resolve_hash(operation):
                    return None
            h = self.hashes.get(operation)
            ext = json.dumps({"persistedQuery": {"version": 1, "sha256Hash": h}})
            url = f"https://www.airbnb.com/api/v3/{operation}/{h}"
            base_params = {"operationName": operation, "locale": self.locale, "currency": self.currency}
            if method == "GET":
                params = {**base_params, "variables": json.dumps(variables, separators=(",", ":")), "extensions": ext}
                res = await self.client.request("GET", url, headers=BASE_HEADERS, params=params)
            else:
                body = {"operationName": operation, "variables": variables,
                        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": h}}}
                res = await self.client.request("POST", url, headers=BASE_HEADERS, params=base_params, json_body=body)
            if not res.ok:
                continue
            try:
                payload = res.json()
            except Exception:
                continue
            errors = payload.get("errors")
            if errors:
                msg = json.dumps(errors)
                if "PersistedQueryNotFound" in msg or "persisted" in msg.lower():
                    continue
                Actor.log.debug(f"[airbnb] {operation} errors: {msg[:200]}")
                return None
            if ok_key and ok_key not in payload:
                continue
            return payload
        return None

    # ---- search ----------------------------------------------------------------------------
    def _raw_params(self, extra: dict) -> list[dict]:
        params = {
            "cdnCacheSafe": "false", "itemsPerGrid": "50", "refinementPaths": ["/homes"],
            "screenSize": "large", "tabId": "home_tab", "version": "1.8.8",
        }
        params.update(extra)
        out = []
        for k, v in params.items():
            out.append({"filterName": k, "filterValues": v if isinstance(v, list) else [str(v)]})
        return out

    async def _search_page(self, extra: dict, cursor: str | None) -> dict | None:
        raw = self._raw_params(extra)
        ssr = {"maxMapItems": 9999, "metadataOnly": False, "rawParams": raw,
               "requestedPageType": "STAYS_SEARCH", "treatmentFlags": TREATMENT_FLAGS,
               "searchType": "user_map_move" if "ne_lat" in extra else "filter_change"}
        if cursor:
            ssr["cursor"] = cursor
        map_raw = [p for p in raw if p["filterName"] != "itemsPerGrid"]
        variables = {
            "aiSearchEnabled": False, "isLeanTreatment": False, "staysSearchRequest": ssr,
            "staysMapSearchRequestV2": {"metadataOnly": False, "rawParams": map_raw,
                                        "requestedPageType": "STAYS_SEARCH", "treatmentFlags": TREATMENT_FLAGS,
                                        **({"cursor": cursor} if cursor else {})},
            "includeMapResults": True, "skipExtendedSearchParams": False,
        }
        payload = await self._gql("StaysSearch", variables, method="POST")
        if not payload:
            return None
        try:
            return payload["data"]["presentation"]["staysSearch"]
        except (KeyError, TypeError):
            return None

    async def _collect_tile(self, extra: dict, remaining: int, max_pages: int) -> tuple[dict, dict | None, bool]:
        """Return (listings_by_id, map_bounds_hint, saturated) for a single tile/query."""
        found: dict[str, dict] = {}
        stays = await self._search_page(extra, None)
        if not stays:
            return found, None, False
        results = stays.get("results") or {}
        hint = None
        try:
            hint = stays["mapResults"]["mapMetadata"].get("mapBoundsHint")
        except (KeyError, TypeError):
            hint = None
        cursors = (results.get("paginationInfo") or {}).get("pageCursors") or []
        for item in results.get("searchResults") or []:
            row = parse_search_result(item)
            if row:
                found[row["id"]] = row
        saturated = len(cursors) >= 15
        pages = min(len(cursors), max_pages)
        for idx in range(1, pages):
            if len(found) >= remaining:
                break
            stays = await self._search_page(extra, cursors[idx])
            if not stays:
                break
            for item in (stays.get("results") or {}).get("searchResults") or []:
                row = parse_search_result(item)
                if row:
                    found[row["id"]] = row
        return found, hint, saturated

    async def search(self, *, query: str | None = None, bounds: dict | None = None,
                     extra_filters: dict | None = None, max_listings: int = 500,
                     max_tiles: int = 40, max_pages: int = 15, subdivide: bool = True):
        """Yield unique listing rows for a market. bounds = {ne_lat,ne_lng,sw_lat,sw_lng}."""
        extra_filters = extra_filters or {}
        seen: set[str] = set()

        def take(rows: dict):
            """Yield only the not-yet-seen rows, stopping exactly at max_listings."""
            for lid, row in rows.items():
                if len(seen) >= max_listings:
                    return
                if lid not in seen:
                    seen.add(lid)
                    yield row

        # Seed bounds: explicit bounds, else derive from a query search's map hint.
        seed_bounds = None
        if bounds:
            seed_bounds = bounds
        elif query:
            rows, hint, _ = await self._collect_tile({"query": query, **extra_filters}, max_listings, max_pages)
            for row in take(rows):
                yield row
            if hint:
                seed_bounds = {
                    "ne_lat": hint["northeast"]["latitude"], "ne_lng": hint["northeast"]["longitude"],
                    "sw_lat": hint["southwest"]["latitude"], "sw_lng": hint["southwest"]["longitude"],
                }
            if len(seen) >= max_listings or seed_bounds is None:
                return

        if seed_bounds is None:
            return

        tiles = [(seed_bounds, 0)]
        processed = 0
        max_depth = 6 if subdivide else 0
        while tiles and len(seen) < max_listings and processed < max_tiles:
            tile, depth = tiles.pop(0)
            processed += 1
            extra = {
                "ne_lat": tile["ne_lat"], "ne_lng": tile["ne_lng"],
                "sw_lat": tile["sw_lat"], "sw_lng": tile["sw_lng"],
                "search_by_map": "true", "zoom_level": str(min(19, 10 + depth * 2)),
                **({"query": query} if query else {}), **extra_filters,
            }
            rows, _, saturated = await self._collect_tile(extra, max_listings - len(seen), max_pages)
            for row in take(rows):
                yield row
            if saturated and depth < max_depth and len(seen) < max_listings:
                tiles.extend((q, depth + 1) for q in _quadrants(tile))

    # ---- detail, calendar, pricing ---------------------------------------------------------
    async def detail(self, listing_id: str) -> dict | None:
        url = f"https://www.airbnb.com/rooms/{listing_id}"
        res = await self.client.request("GET", url, headers={
            "accept-language": "en-US,en;q=0.9",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        }, ok_predicate=lambda s, t: s == 200 and "data-deferred-state" in t)
        if not res.ok:
            return None
        m = re.search(r'<script id="data-deferred-state-0"[^>]*>(.*?)</script>', res.text, re.S)
        if not m:
            return None
        try:
            state = json.loads(m.group(1))
            node = state["niobeClientData"][0][1]
        except Exception:
            return None
        return parse_pdp(node, listing_id)

    async def calendar(self, listing_id: str, months: int = 12) -> list[dict] | None:
        import datetime

        today = datetime.date.today()
        variables = {"request": {"count": months, "listingId": str(listing_id),
                                 "month": today.month, "year": today.year}}
        payload = await self._gql("PdpAvailabilityCalendar", variables, method="GET")
        if not payload:
            return None
        try:
            cal = payload["data"]["merlin"]["pdpAvailabilityCalendar"]["calendarMonths"]
        except (KeyError, TypeError):
            return None
        days = []
        for month in cal:
            for d in month.get("days") or []:
                days.append({
                    "date": d.get("calendarDate"),
                    "available": bool(d.get("available")),
                    "minNights": d.get("minNights"),
                    "maxNights": d.get("maxNights"),
                    "bookable": d.get("bookable"),
                })
        return days

    async def price_for_dates(self, listing_id: str, checkin: str, checkout: str, adults: int = 1) -> dict | None:
        enc = base64.b64encode(f"DemandStayListing:{listing_id}".encode()).decode()
        variables = {
            "id": enc, "dateRange": {"startDate": checkin, "endDate": checkout},
            "guestCounts": {"numberOfAdults": adults},
            "includePdpMigrationBookItCalendarSheetFragment": True,
            "includePdpMigrationBookItFloatingFooterFragment": True,
            "includePdpMigrationBookItNavFragment": True,
            "includePdpMigrationBookItSidebarFragment": True,
            "includeOverviewMerchandisingTipsFragment": False,
            "includeStaysPdpPriceHeatmapFragment": False,
            "priceHeatmapDateRange": {"startDate": checkin, "endDate": checkout},
            "p3ImpressionId": "p3_1_A", "selectedCancellationPolicyId": None,
            "causeId": None, "selectedGuestOptionId": None,
        }
        payload = await self._gql("StaysPdpBookItQuery", variables, method="GET")
        if not payload:
            return None
        try:
            book = payload["data"]["node"]["pdpPresentation"]["bookIt"]
        except (KeyError, TypeError):
            return None
        return parse_bookit(book, checkin, checkout)


def _quadrants(b: dict) -> list[dict]:
    mid_lat = (b["ne_lat"] + b["sw_lat"]) / 2
    mid_lng = (b["ne_lng"] + b["sw_lng"]) / 2
    return [
        {"ne_lat": b["ne_lat"], "ne_lng": mid_lng, "sw_lat": mid_lat, "sw_lng": b["sw_lng"]},
        {"ne_lat": b["ne_lat"], "ne_lng": b["ne_lng"], "sw_lat": mid_lat, "sw_lng": mid_lng},
        {"ne_lat": mid_lat, "ne_lng": mid_lng, "sw_lat": b["sw_lat"], "sw_lng": b["sw_lng"]},
        {"ne_lat": mid_lat, "ne_lng": b["ne_lng"], "sw_lat": b["sw_lat"], "sw_lng": mid_lng},
    ]


def _rating_parts(label: str | None):
    if not label:
        return None, None
    m = _RATING_RE.search(label.replace("\xa0", " "))
    if not m:
        return None, None
    try:
        rating = float(m.group(1).replace(",", "."))
    except ValueError:
        rating = None
    try:
        count = int(m.group(2).replace(",", ""))
    except ValueError:
        count = None
    return rating, count


def _structured_lines(structured: dict, keys: set[str]) -> list[str]:
    out = []
    for line in structured.get("primaryLine") or []:
        if line.get("type") in keys and line.get("body"):
            out.append(line["body"])
    return out


def parse_search_result(item: dict) -> dict | None:
    dsl = item.get("demandStayListing") or {}
    lid = _decode_listing_id(dsl.get("id"))
    if not lid:
        return None
    coord = (dsl.get("location") or {}).get("coordinate") or {}
    rating, reviews = _rating_parts(item.get("avgRatingA11yLabel") or item.get("avgRatingLocalized"))
    structured = item.get("structuredContent") or {}
    price = item.get("structuredDisplayPrice") or {}
    primary = price.get("primaryLine") or {}
    badges = [b.get("text") for b in item.get("badges") or [] if b.get("text")]
    images = [p.get("picture") for p in item.get("contextualPictures") or [] if p.get("picture")]
    name = item.get("name") or item.get("title")
    if isinstance(item.get("nameLocalized"), dict):
        name = item["nameLocalized"].get("localizedStringWithTranslationPreference") or name
    return {
        "platform": "airbnb",
        "id": lid,
        "url": f"https://www.airbnb.com/rooms/{lid}",
        "name": name,
        "propertyType": item.get("title"),
        "coordinates": {"lat": coord.get("latitude"), "lng": coord.get("longitude")} if coord else None,
        "rating": rating,
        "reviewsCount": reviews,
        "roomInfo": _structured_lines(structured, {"BEDINFO", "BATHROOMINFO"}),
        "priceLabel": primary.get("discountedPrice") or primary.get("price"),
        "originalPriceLabel": primary.get("originalPrice"),
        "priceQualifier": primary.get("qualifier"),
        "badges": badges,
        "isGuestFavorite": any("GUEST_FAVORITE" in json.dumps(b) for b in item.get("badges") or []),
        "images": images[:12],
        "thumbnail": images[0] if images else None,
    }


def parse_pdp(node: dict, listing_id: str) -> dict | None:
    data = node.get("data") or {}
    n = data.get("node") or {}
    pdp = n.get("pdpPresentation") or {}
    metadata = {}
    try:
        metadata = data["presentation"]["stayProductDetailPage"]["sections"]["metadata"]
    except (KeyError, TypeError):
        metadata = {}
    logging_ctx = ((metadata.get("loggingContext") or {}).get("eventDataLogging") or {})
    sharing = metadata.get("sharingConfig") or {}
    seo = metadata.get("seoFeatures") or {}

    loc = pdp.get("location") or {}
    coord = (n.get("location") or {}).get("coordinate") or {}
    host = (pdp.get("hostInfo") or {})
    passport = host.get("passportData") or {}
    overview = pdp.get("overview") or {}
    quality = pdp.get("quality") or {}
    rating_stats = (quality.get("listingRatingStats") or {}).get("overallRatingStats") or {}

    amenities = []
    for group in (pdp.get("amenities") or {}).get("seeAllAmenitiesGroups") or []:
        for a in group.get("amenities") or []:
            if a.get("available") and a.get("title"):
                amenities.append(a["title"])

    photos = []
    for stop in (pdp.get("mediaTour") or {}).get("stops") or []:
        for it in stop.get("items") or []:
            img = it.get("image") or {}
            if img.get("uri"):
                photos.append(img["uri"])

    house_rules = []
    for group in (pdp.get("rules") or {}).get("groupItems") or []:
        for it in group.get("items") or []:
            if it.get("title"):
                house_rules.append(it["title"])

    description = None
    desc = pdp.get("descriptions") or {}
    if isinstance(desc.get("longDescriptionHtml"), dict):
        description = desc["longDescriptionHtml"].get("localizedStringWithTranslationPreference")

    category_ratings = {}
    for cat in quality.get("categoryRatings") or []:
        if cat.get("categoryType") and cat.get("localizedRating"):
            category_ratings[cat["categoryType"].lower()] = _to_float(cat["localizedRating"])

    return {
        "platform": "airbnb",
        "id": listing_id,
        "url": f"https://www.airbnb.com/rooms/{listing_id}",
        "name": _ugc(pdp.get("title")) or (n.get("description") or {}).get("name", {}).get("localizedStringWithTranslationPreference"),
        "propertyType": sharing.get("propertyType") or n.get("propertyType"),
        "roomType": logging_ctx.get("roomType"),
        "spaceType": n.get("spaceType"),
        "personCapacity": n.get("personCapacity") or sharing.get("personCapacity"),
        "overviewItems": overview.get("items"),
        "coordinates": {"lat": coord.get("latitude") or loc.get("latitude"),
                        "lng": coord.get("longitude") or loc.get("longitude")},
        "isExactLocation": loc.get("isExactLocation"),
        "locationSubtitle": loc.get("subtitle"),
        "rating": rating_stats.get("ratingAverage") or logging_ctx.get("guestSatisfactionOverall"),
        "reviewsCount": _to_int(rating_stats.get("ratingCount")) or _to_int(logging_ctx.get("visibleReviewCount")),
        "categoryRatings": category_ratings or None,
        "isSuperhost": passport.get("isSuperhost"),
        "isGuestFavorite": quality.get("isGuestFavorite"),
        "host": {
            "name": passport.get("name"),
            "isSuperhost": passport.get("isSuperhost"),
            "isVerified": passport.get("isVerified"),
            "ratingCount": passport.get("ratingCount"),
            "ratingAverage": passport.get("ratingAverage"),
            "yearsHosting": (passport.get("timeAsHost") or {}).get("years"),
            "responseRate": host.get("responseRateText"),
            "responseTime": host.get("responseTimeText"),
            "profileUrl": f"https://www.airbnb.com/users/show/{_decode_user_id(passport.get('userId'))}"
            if passport.get("userId") else None,
        },
        "amenities": amenities,
        "amenitiesCount": len(amenities),
        "houseRules": house_rules,
        "description": description,
        "images": photos[:40],
        "thumbnail": (photos[0] if photos else sharing.get("imageUrl")),
        "seoTitle": (seo.get("title") if isinstance(seo.get("title"), str) else None),
        "metaDescription": seo.get("metaDescription") if isinstance(seo.get("metaDescription"), str) else None,
    }


def parse_bookit(book: dict, checkin: str, checkout: str) -> dict:
    sdp = book.get("structuredDisplayPrice") or {}
    primary = sdp.get("primaryLine") or {}
    availability = book.get("availability") or {}
    line_items = []
    total = None
    for group in ((sdp.get("explanationData") or {}).get("priceDetails") or []):
        for it in group.get("items") or []:
            if it.get("description") and it.get("priceString"):
                line_items.append({"label": it["description"], "amount": it["priceString"]})
            if it.get("__typename") == "HighlightExplanationLineItem":
                total = it.get("priceString")
    return {
        "checkIn": checkin,
        "checkOut": checkout,
        "isAvailable": availability.get("isAvailable"),
        "canInstantBook": availability.get("canInstantBook"),
        "displayPrice": primary.get("discountedPrice") or primary.get("price"),
        "originalPrice": primary.get("originalPrice"),
        "totalBeforeTaxes": total,
        "priceItems": line_items,
    }


def _ugc(v):
    if isinstance(v, dict):
        content = v.get("content") or v
        return content.get("localizedStringWithTranslationPreference") or content.get("localizedString")
    return v


def _decode_user_id(encoded):
    return _decode_listing_id(encoded)


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