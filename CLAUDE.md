# TopDownHockey_Scraper

**PyPI Package**: `pip install TopDownHockey_Scraper`

This is a Python package for scraping hockey data from two sources:

1. **NHL Play-by-Play** - Scrapes NHL API for game events, shifts, and rosters
2. **Elite Prospects** - Scrapes skater/goalie stats from thousands of global leagues

## Relationship to Other Projects

This package is a **dependency** used by both:
- `compact_topdownhockey/` - Uses `full_scrape()` in nightly pipeline to fetch PBP data
- `player-cards/api/` - Uses `full_scrape()` for live game scraping when games aren't in Supabase yet

## Key Modules

| Module | Import | Purpose |
|--------|--------|---------|
| NHL Scraper | `import TopDownHockey_Scraper.TopDownHockey_NHL_Scraper as tdhnhlscrape` | NHL PBP data |
| Elite Prospects | `import TopDownHockey_Scraper.TopDownHockey_EliteProspects_Scraper as tdhepscrape` | Global leagues |

## Common Functions

```python
# Get game schedule
tdhnhlscrape.scrape_full_schedule(start_date, end_date)

# Scrape play-by-play for games
tdhnhlscrape.full_scrape([2024020001, 2024020002])

# Get skater stats from Elite Prospects
tdhepscrape.get_skaters(("nhl", "ahl"), ("2023-2024", "2024-2025"))
```

## Development

```bash
cd ~/packaging_tutorial

# Install locally for development
pip install -e .

# Run tests
pytest tests/

# Publish to PyPI (bump version in setup.py first)
python -m build
twine upload dist/*
```

## Files

- `src/TopDownHockey_Scraper/TopDownHockey_NHL_Scraper.py` - Main NHL scraping logic
- `src/TopDownHockey_Scraper/TopDownHockey_EliteProspects_Scraper.py` - EP scraping
- `src/TopDownHockey_Scraper/portrait_links.csv` - Player headshot URL mappings
- `src/TopDownHockey_Scraper/data/handedness.csv` - Player handedness data
