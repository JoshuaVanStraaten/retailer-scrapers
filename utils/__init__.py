"""Utility modules for Savvy Grocery Scraper."""

from .common import (
    # HTTP utilities
    get_random_user_agent,
    create_session_with_retries,
    make_request_with_retry,

    # Data processing
    normalize_product_name,
    extract_size_info,
    parse_price,
    sanitize_filename,
    generate_product_id,

    # Image handling
    download_image,
    upload_image_to_supabase,
    process_product_image,

    # Supabase
    get_supabase_client,
    upsert_to_supabase,

    # CSV utilities
    load_csv_as_dict,
    save_to_csv,
    deduplicate_csv,

    # Logging
    setup_logger,

    # Timing
    Timer,
    rate_limited,
)

from .notifications import (
    DiscordNotifier,
    NotificationType,
    notifier,
    notify_started,
    notify_completed,
    notify_error,
    notify_summary,
)

from .session_manager import (
    SessionData,
    PlaywrightSessionManager,
    get_session,
)

from .image_lookup import (
    ImageLookup,
    image_lookup,
    init_image_lookup,
    normalize_product_name as normalize_for_image,
)

__all__ = [
    # HTTP utilities
    "get_random_user_agent",
    "create_session_with_retries",
    "make_request_with_retry",

    # Data processing
    "normalize_product_name",
    "extract_size_info",
    "parse_price",
    "sanitize_filename",
    "generate_product_id",

    # Image handling
    "download_image",
    "upload_image_to_supabase",
    "process_product_image",

    # Supabase
    "get_supabase_client",
    "upsert_to_supabase",

    # CSV utilities
    "load_csv_as_dict",
    "save_to_csv",
    "deduplicate_csv",

    # Logging
    "setup_logger",

    # Timing
    "Timer",
    "rate_limited",

    # Notifications
    "DiscordNotifier",
    "NotificationType",
    "notifier",
    "notify_started",
    "notify_completed",
    "notify_error",
    "notify_summary",

    # Session management
    "SessionData",
    "PlaywrightSessionManager",
    "get_session",

    # Image lookup
    "ImageLookup",
    "image_lookup",
    "init_image_lookup",
    "normalize_for_image",
]