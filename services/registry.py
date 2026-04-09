"""Channel labels, connector mappings, and API dispatch table."""

from __future__ import annotations

CHANNEL_LABELS = {
    "google_youtube": "Google Ads (Google + YouTube)",
    "meta": "Meta (Facebook + Instagram)",
    "microsoft": "Microsoft / Bing",
    "pinterest": "Pinterest",
    "snapchat": "Snapchat",
    "tiktok": "TikTok",
    "twitter_x": "Twitter / X",
    "amazon": "Amazon Ads",
    "reddit": "Reddit Ads",
    "vibe": "Vibe",
}

# Map Datastore "connector" values to internal channel keys.
CONNECTOR_TO_CHANNEL = {
    "google": "google_youtube",
    "google_ads": "google_youtube",
    "google_youtube": "google_youtube",
    "google_customer_match": "google_youtube",
    "meta": "meta",
    "facebook": "meta",
    "facebook_ads": "meta",
    "facebook_custom_audience": "meta",
    "microsoft": "microsoft",
    "bing": "microsoft",
    "microsoft_bing": "microsoft",
    "microsoft_ads": "microsoft",
    "pinterest": "pinterest",
    "snapchat": "snapchat",
    "tiktok": "tiktok",
    "tiktok_ads": "tiktok",
    "twitter": "twitter_x",
    "twitter_x": "twitter_x",
    "x": "twitter_x",
    "amazon": "amazon",
    "amazon_ads": "amazon",
    "reddit": "reddit",
    "reddit_ads": "reddit",
    "vibe": "vibe",
}


def _get_dispatch():
    """Lazy-load dispatch table to avoid circular imports."""
    from services.platforms.google import fetch_google_youtube_api_daily
    from services.platforms.meta import fetch_meta_api_daily
    from services.platforms.microsoft import fetch_microsoft_api_daily
    from services.platforms.pinterest import fetch_pinterest_api_daily
    from services.platforms.snapchat import fetch_snapchat_api_daily
    from services.platforms.tiktok import fetch_tiktok_api_daily
    from services.platforms.twitter import fetch_twitter_x_api_daily
    from services.platforms.amazon import fetch_amazon_api_daily
    from services.platforms.reddit import fetch_reddit_api_daily
    from services.platforms.vibe import fetch_vibe_api_daily

    return {
        "google_youtube": fetch_google_youtube_api_daily,
        "meta": fetch_meta_api_daily,
        "microsoft": fetch_microsoft_api_daily,
        "pinterest": fetch_pinterest_api_daily,
        "snapchat": fetch_snapchat_api_daily,
        "tiktok": fetch_tiktok_api_daily,
        "twitter_x": fetch_twitter_x_api_daily,
        "amazon": fetch_amazon_api_daily,
        "reddit": fetch_reddit_api_daily,
        "vibe": fetch_vibe_api_daily,
    }


# Populated on first access
CHANNEL_API_DISPATCH = None


def get_dispatch_table():
    global CHANNEL_API_DISPATCH
    if CHANNEL_API_DISPATCH is None:
        CHANNEL_API_DISPATCH = _get_dispatch()
    return CHANNEL_API_DISPATCH
