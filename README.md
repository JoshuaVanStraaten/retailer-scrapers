# Retailer scrapers

A Python-based web scraping system for collecting product data from major South African grocery retailers. Built for the [Milk](https://github.com/JoshuaVanStraaten/milk) price comparison app.

## Features

- **4 Retailers**: Checkers, Shoprite, Pick n Pay, Woolworths
- **9 Provinces**: Full coverage across South Africa
- **~33,000+ products** per province
- **Smart Image Caching**: Avoids re-downloading existing images
- **Automatic Retries**: Handles connection failures gracefully
- **Discord Notifications**: Get alerts on completion/errors
- **Supabase Integration**: Direct upload to database and storage

## Architecture

```
retailer-scrapers/
├── config/
│   ├── __init__.py
│   └── settings.py          # Centralized configuration
├── scrapers/
│   ├── __init__.py
│   ├── base.py              # Base scraper class
│   ├── checkers.py          # Checkers scraper (Playwright sessions)
│   ├── shoprite.py          # Shoprite scraper (Playwright sessions)
│   ├── pnp.py               # Pick n Pay scraper (JSON API)
│   └── woolworths.py        # Woolworths scraper (JSON API)
├── utils/
│   ├── __init__.py
│   ├── common.py            # Shared utilities
│   ├── image_lookup.py      # Image URL cache system
│   ├── notifications.py     # Discord notifications
│   └── session_manager.py   # Playwright session management
├── data/
│   └── image_lookup_cache.json  # Cached image URLs (commit this!)
├── main.py                  # CLI entry point
├── requirements.txt
├── .env.example
└── README.md
```

## Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/JoshuaVanStraaten/retailer-scrapers.git
cd retailer-scrapers

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers (for Checkers/Shoprite)
playwright install chromium
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

Required environment variables:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...  # Optional
```

### 3. Run Scrapers

```bash
# All retailers, all provinces
python main.py

# Specific retailers
python main.py --retailers checkers shoprite

# Specific provinces
python main.py --provinces Gauteng "Western Cape"

# Quick test (no upload)
python main.py --retailers pnp --provinces Gauteng --no-upload
```

## CLI Options

| Option              | Description                                                      |
| ------------------- | ---------------------------------------------------------------- |
| `--retailers`       | Retailers to scrape: `checkers`, `shoprite`, `pnp`, `woolworths` |
| `--provinces`       | Provinces to scrape (default: all 9)                             |
| `--no-upload`       | Skip Supabase upload                                             |
| `--sequential`      | Disable concurrent page scraping                                 |
| `--validate-config` | Check configuration and exit                                     |

## Retailer Details

| Retailer   | Method      | Products/Page | Session Required  |
| ---------- | ----------- | ------------- | ----------------- |
| Checkers   | HTML + CSRF | 20            | Yes (Playwright)  |
| Shoprite   | HTML + CSRF | 20            | Yes (Playwright)  |
| Pick n Pay | JSON API    | 72            | No                |
| Woolworths | JSON API    | 24            | No (confirmPlace) |

## Image Handling

The scraper uses a smart caching system to avoid re-downloading images:

1. **Check Cache**: Image in `image_lookup_cache.json`? Use cached Supabase URL
2. **Check Config**: `USE_CDN_URLS=true`? Use retailer CDN directly (no upload)
3. **Download**: Only if not in cache - uploads to Supabase, saves URL to cache

**Important**: Commit `data/image_lookup_cache.json` - it contains 23k+ image URLs and saves hours of re-downloading!

### CSV Behavior

The CSV files (`products_*.csv`) are **temporary/intermediate**:

- Rebuilt fresh each run (overwritten, not appended)
- Used for batch upload to Supabase
- No duplicates - complete rebuild each time
- **Don't commit** - they're regenerated

## Province Store Codes

<details>
<summary>Checkers Stores</summary>

| Province      | Store Code |
| ------------- | ---------- |
| Western Cape  | 6551       |
| Gauteng       | 7303       |
| KwaZulu-Natal | 40206      |
| Northern Cape | 52798      |
| Eastern Cape  | 30562      |
| Free State    | 7727       |
| North West    | 52772      |
| Limpopo       | 85363      |
| Mpumalanga    | 45036      |

</details>

<details>
<summary>Shoprite Stores</summary>

| Province      | Store Code |
| ------------- | ---------- |
| Western Cape  | 46197      |
| Northern Cape | 880        |
| Eastern Cape  | 6292       |
| KwaZulu-Natal | 6721       |
| Free State    | 52837      |
| Gauteng       | 814        |
| Mpumalanga    | 44933      |
| North West    | 953        |
| Limpopo       | 52691      |

</details>

<details>
<summary>Pick n Pay Stores</summary>

| Province      | Store Code |
| ------------- | ---------- |
| Western Cape  | WC44       |
| Eastern Cape  | EC29       |
| Northern Cape | GC61       |
| Free State    | GH45       |
| North West    | HC08       |
| Gauteng       | GC13       |
| KwaZulu-Natal | KC16       |
| Limpopo       | NC38       |
| Mpumalanga    | NC12       |

</details>

<details>
<summary>Woolworths Stores</summary>

| Province      | Nickname            | Place ID                    |
| ------------- | ------------------- | --------------------------- |
| Gauteng       | 2 Saltus Street     | ChIJt3cT6lVmlR4RQhVr-hreuuU |
| Western Cape  | 210 Paarl Rock Rd   | EiYyMTAgUGFhcmwg...         |
| Eastern Cape  | 1 Ring Road         | ChIJ5aSUWdLTeh4RK...        |
| KwaZulu-Natal | 1 Premium Promenade | ChIJ4SJXgjkj-h4RaBu...      |

_More provinces can be added by finding valid Google Place IDs_

</details>

## Performance

| Retailer   | Products (1 province) | Time        |
| ---------- | --------------------- | ----------- |
| Checkers   | ~7,500                | ~20 min     |
| Shoprite   | ~7,500                | ~20 min     |
| Pick n Pay | ~10,000               | ~15 min     |
| Woolworths | ~8,000                | ~16 min     |
| **Total**  | **~33,000**           | **~70 min** |

_Times vary based on network and Supabase response times_

## Database Schema

The scraper uploads to a `Products` table with this structure:

```sql
CREATE TABLE Products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price TEXT,
    promotion_price TEXT,
    retailer TEXT NOT NULL,
    image_url TEXT,
    promotion_valid TEXT,
    province TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(name, retailer, province)
);
```

## Troubleshooting

### "Server disconnected" errors

- Normal during large uploads
- Automatic retry handles most cases
- If persistent, check your internet connection

### "403 Forbidden" errors

- Session expired (Checkers/Shoprite)
- Automatic session refresh handles this
- Delete `data/sessions/` folder to force new sessions

### Missing promo dates (Woolworths)

- Woolworths changes their date format periodically
- Check the regex patterns in `scrapers/woolworths.py`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with `--no-upload` flag
5. Submit a pull request

## License

MIT License - See [LICENSE](LICENSE) for details.

## Related Projects

- [Milk App](https://github.com/JoshuaVanStraaten/milk) - Flutter mobile app
