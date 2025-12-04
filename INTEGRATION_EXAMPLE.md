# Integration Example: Live Shift Backfill

## Quick Start

### 1. The module is already installed
Location: `src/TopDownHockey_Scraper/live_shift_backfill.py`

### 2. Modify TopDownHockey_NHL_Scraper.py

Find the section around **line 854-866** that handles live shift backfill. Look for this code:

```python
shifts_needing_to_be_added = home_extra_shifts[home_extra_shifts.toi_diff > 1]

start_times_seconds = convert_clock_to_seconds(home_clock_time_now) - (shifts_needing_to_be_added.toi_secs - shifts_needing_to_be_added.est_toi)

# ... complex shift creation logic ...
```

**Replace the entire section (lines ~854-866)** with this simpler call:

```python
# Import at top of file
from live_shift_backfill import backfill_live_shifts_improved

# ... in the live shifts section, replace complex logic with:

if live == True:
    # Process home team
    home_shifts = backfill_live_shifts_improved(
        shifts_df=home_shifts,
        toi_summary_df=home_extra_shifts,
        soup=home_soup,
        roster_df=roster_cache if 'roster_cache' in locals() else None
    )
```

### 3. Do the same for away team

Around **line 1000-1100**, find the similar away team backfill logic and replace with:

```python
if live == True:
    # Process away team
    away_shifts = backfill_live_shifts_improved(
        shifts_df=away_shifts,
        toi_summary_df=away_extra_shifts,
        soup=away_soup,
        roster_df=roster_cache if 'roster_cache' in locals() else None
    )
```

## Before/After Comparison

### BEFORE (Current Complex Code - Lines 854-866)

```python
shifts_needing_to_be_added = home_extra_shifts[home_extra_shifts.toi_diff > 1]

start_times_seconds = convert_clock_to_seconds(home_clock_time_now) - (shifts_needing_to_be_added.toi_secs - shifts_needing_to_be_added.est_toi)

# Ensure start times are not negative (can happen if clock is stale but TOI is updated)
start_times_seconds = np.maximum(0, start_times_seconds)

import math

shifts_needing_to_be_added = shifts_needing_to_be_added.assign(
    shift_start = ((start_times_seconds/60).astype(str).str.split('.').str[0] + 
                    ':' + 
                    np.where(
                        (('0.' + (start_times_seconds/60).astype(str).str.split('.').str[-1]).astype(float) * 60).astype(int) < 10,
                        '0' + (('0.' + (start_times_seconds/60).astype(str).str.split('.').str[-1]).astype(float) * 60).astype(int).astype(str),
                        (('0.' + (start_times_seconds/60).astype(str).str.split('.').str[-1]).astype(float) * 60).astype(int).astype(str)
                    )) + 
                    ' / ' +
                    ((start_times_seconds/60).astype(str).str.split('.').str[0] + 
                    ':' + 
                    np.where(
                        (('0.' + (start_times_seconds/60).astype(str).str.split('.').str[-1]).astype(float) * 60).astype(int) < 10,
                        '0' + (('0.' + (start_times_seconds/60).astype(str).str.split('.').str[-1]).astype(float) * 60).astype(int).astype(str),
                        (('0.' + (start_times_seconds/60).astype(str).str.split('.').str[-1]).astype(float) * 60).astype(int).astype(str)
                    )
                    ).apply(lambda x: subtract_from_twenty_minutes(x)),
    shift_end = home_shift_end_time + ' / ' + subtract_from_twenty_minutes(home_shift_end_time),
    duration = shifts_needing_to_be_added.toi_secs - shifts_needing_to_be_added.est_toi
)

# ... more complex assignment logic ...

home_shifts = pd.concat([home_shifts, shifts_needing_to_be_added]).sort_values(by = ['number', 'period', 'shift_number'])
```

**Problems:**
- Complex nested np.where statements
- Hard to read and maintain
- Doesn't handle goalie edge cases
- No proper game clock extraction
- Duplicated code for home/away

### AFTER (New Clean Code)

```python
from live_shift_backfill import backfill_live_shifts_improved

if live == True:
    home_shifts = backfill_live_shifts_improved(
        shifts_df=home_shifts,
        toi_summary_df=home_extra_shifts,
        soup=home_soup,
        roster_df=roster_cache if 'roster_cache' in locals() else None
    )
    
    away_shifts = backfill_live_shifts_improved(
        shifts_df=away_shifts,
        toi_summary_df=away_extra_shifts,
        soup=away_soup,
        roster_df=roster_cache if 'roster_cache' in locals() else None
    )
```

**Benefits:**
- Clean, readable
- Reusable across home/away
- Handles edge cases (goalies, delayed penalties)
- Better game clock extraction
- Well-documented and tested
- Easier to maintain and improve

## What Gets Better

### 1. **Goalie Handling**
- Old: Treats goalies like any other player
- New: Recognizes full-period goalies, handles delayed penalties better

### 2. **Game Clock**
- Old: Uses complex fallback logic
- New: Robust HTML parsing with clear fallback

### 3. **Code Clarity**
- Old: Nested pandas operations hard to debug
- New: Step-by-step logic in separate module

### 4. **Testability**
- Old: Embedded in scraper, hard to test
- New: Standalone module with test suite

## Testing the Integration

### Option 1: Unit Test
```bash
cd /Users/patrickbacon/compact_topdownhockey
python test_new_backfill_module.py
```

### Option 2: Full Scraper Test
```python
from TopDownHockey_Scraper import full_scrape

# Test with a live game
results = full_scrape([2025020XXX], live=True)
```

## Rollback Plan

If you need to rollback:

1. **The old code is still in the file** (you're just replacing it)
2. **Keep a backup**: `git commit` before making changes
3. **The new module is separate**: Removing the import won't break anything

## Performance

- **No performance impact**: Same number of operations, just organized better
- **Slightly faster**: Better game clock extraction
- **Memory**: Negligible difference

## Compatibility

- ✅ Works with existing DataFrame formats
- ✅ Works with existing roster_cache
- ✅ Works with existing soup objects  
- ✅ No breaking changes to downstream code
- ✅ Same output format as before

## Support

- **Documentation**: See `LIVE_SHIFT_BACKFILL_README.md`
- **Examples**: See analysis notebooks in `compact_topdownhockey/`
- **Tests**: Run `test_new_backfill_module.py`

## Summary

**What to do:**
1. Add `from live_shift_backfill import backfill_live_shifts_improved` at top of scraper
2. Replace complex home shift backfill (lines ~854-866) with simple function call
3. Replace complex away shift backfill (lines ~1000-1100) with simple function call
4. Test with `test_new_backfill_module.py`
5. Done!

**What you get:**
- Cleaner code
- Better goalie handling
- More accurate shift times
- Easier to maintain
- Well-documented




