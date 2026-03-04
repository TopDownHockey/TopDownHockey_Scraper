"""
API-based shift processing logic, using the NHL REST API for shift data.

Replaces the HTML shift report scraping with a single API call to:
https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}

Produces the same output format as shift_processing.scrape_html_shifts() so it
can be used as a drop-in replacement in the scraper pipeline.
"""

import numpy as np
import pandas as pd
import requests
import re
import time
from datetime import datetime

from TopDownHockey_Scraper.name_corrections import normalize_player_name

# Import helper functions from the main scraper module
from TopDownHockey_Scraper.TopDownHockey_NHL_Scraper import (
    convert_clock_to_seconds,
    convert_seconds_to_clock,
    subtract_from_twenty_minutes,
    _session,
    _log_exception_with_dataframe,
)


# NHL Shifts REST API base URL
_SHIFTS_API_URL = 'https://api.nhle.com/stats/rest/en/shiftcharts'


def _fetch_shifts_from_api(full_game_id, verbose=False):
    """
    Fetch shift data from the NHL REST API.

    Args:
        full_game_id: Full NHL game ID (e.g., 2025020893)
        verbose: Print timing info

    Returns:
        List of shift records (dicts) from the API
    """
    url = f'{_SHIFTS_API_URL}?cayenneExp=gameId={full_game_id}'

    start = time.time()
    response = _session.get(url, timeout=30)
    response.raise_for_status()
    duration = time.time() - start

    if verbose:
        print(f'  API shifts fetch: {duration:.2f}s')

    data = response.json()
    return data.get('data', [])


def _api_shifts_to_dataframe(shift_records, roster_cache, verbose=False):
    """
    Convert raw API shift records into a DataFrame of individual player shifts,
    in the same format as what the HTML parser produces.

    Args:
        shift_records: List of dicts from the API
        roster_cache: Roster DataFrame with team/team_name/Name/Pos columns

    Returns:
        Tuple of (all_shifts DataFrame, home_team_name, away_team_name)
    """
    if not shift_records:
        return pd.DataFrame(), '', ''

    # Filter to actual shifts (typeCode=517) with non-zero duration
    shifts_only = [r for r in shift_records if r.get('typeCode') == 517
                   and r.get('duration') is not None
                   and r.get('duration') != '00:00']

    if not shifts_only:
        raise IndexError('This game has no shift data.')

    df = pd.DataFrame(shifts_only)

    # Determine home/away team mapping from roster_cache
    home_team_name = roster_cache[roster_cache.team == 'home'].team_name.iloc[0]
    away_team_name = roster_cache[roster_cache.team == 'away'].team_name.iloc[0]

    # Normalize team names for matching
    def _normalize_team(name):
        if 'MONTR' in name.upper() and 'CANAD' in name.upper():
            return 'MONTREAL CANADIENS'
        return name.upper()

    home_team_norm = _normalize_team(home_team_name)
    away_team_norm = _normalize_team(away_team_name)

    # Map API teamName → venue
    def _get_venue(team_name):
        tn = _normalize_team(str(team_name))
        if tn == home_team_norm or ('CANADIENS' in tn and 'CANADIENS' in home_team_norm):
            return 'home'
        elif tn == away_team_norm or ('CANADIENS' in tn and 'CANADIENS' in away_team_norm):
            return 'away'
        # Fallback: try matching on teamId
        return 'unknown'

    df['venue'] = df['teamName'].apply(_get_venue)

    # If venue mapping failed for some rows, try matching via teamId
    if (df['venue'] == 'unknown').any():
        # Use the most common teamId per known venue to fill unknowns
        home_ids = df[df.venue == 'home']['teamId'].unique()
        away_ids = df[df.venue == 'away']['teamId'].unique()
        for tid in df[df.venue == 'unknown']['teamId'].unique():
            if tid in home_ids:
                df.loc[(df.teamId == tid) & (df.venue == 'unknown'), 'venue'] = 'home'
            elif tid in away_ids:
                df.loc[(df.teamId == tid) & (df.venue == 'unknown'), 'venue'] = 'away'

    # Build player name: "FIRST LAST" (uppercased)
    df['name'] = (df['firstName'].str.strip() + ' ' + df['lastName'].str.strip()).str.upper()

    # Map team name to the full name used in the HTML pipeline
    df['team'] = df['venue'].map({'home': home_team_norm, 'away': away_team_norm})

    # Rename to match HTML shift format
    df = df.rename(columns={
        'shiftNumber': 'shift_number',
        'startTime': 'start_time',
        'endTime': 'end_time',
    })

    # Extract jersey number from roster_cache for name matching later
    # The API doesn't give jersey numbers in a convenient format, but we can get
    # them from the roster_cache by matching on player name
    # For now, use playerId and try to map via roster
    # Actually the on_numbers column needs jersey numbers, and the API doesn't include them directly
    # We'll build a player_id → number mapping from the roster
    # The roster has '#' column or 'Pos' etc. Let's check what's available
    # Roster has: '#', 'Pos', 'Name', 'team', 'team_name', 'status'
    roster_number_map = {}
    if '#' in roster_cache.columns:
        for _, row in roster_cache.iterrows():
            roster_number_map[row['Name']] = str(row['#']).strip()

    # Map number using name matching (after we normalize names)
    # For now, keep the number field empty - we'll fill it after name normalization
    df['number'] = ''

    # Convert period: the API uses integer periods (1, 2, 3, 4, 5)
    # Period 4 = OT (regular season), Period 5 = SO
    # Keep as int for now, matching what the HTML version does after normalization
    df['period'] = df['period'].astype(int)

    # Duration from API is "MM:SS" format
    df['duration'] = df['duration'].fillna('0:00')

    # Build shift_start and shift_end in the "elapsed / remaining" format
    # that the downstream code expects... Actually, we DON'T need this format.
    # The downstream code (_build_change_events) only uses start_time and end_time
    # which are already in MM:SS elapsed format from the API.
    # The HTML version creates shift_start/shift_end but then immediately extracts
    # start_time/end_time from them. We can skip that intermediate step.

    # Keep only the columns we need
    result = df[['shift_number', 'period', 'start_time', 'end_time', 'duration',
                 'name', 'number', 'team', 'venue', 'playerId']].copy()

    return result, home_team_norm, away_team_norm


def _apply_name_normalization(all_shifts):
    """Apply all name normalization and corrections to shifts DataFrame."""
    all_shifts['name'] = (all_shifts['name']
        .str.replace('ALEXANDRE ', 'ALEX ', regex=False)
        .str.replace('ALEXANDER ', 'ALEX ', regex=False)
        .str.replace('CHRISTOPHER ', 'CHRIS ', regex=False))

    all_shifts['name'] = all_shifts['name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
    all_shifts['name'] = all_shifts['name'].apply(lambda x: normalize_player_name(x))

    all_shifts['name'] = all_shifts['name'].apply(lambda x: re.sub(r' \(A\)$', '', x).strip())
    all_shifts['name'] = all_shifts['name'].apply(lambda x: re.sub(r' \(C\)$', '', x).strip())

    all_shifts['name'] = np.where(all_shifts['name'] == "JURAJ SLAFKOVSKA", "JURAJ SLAFKOVSKY", all_shifts['name'])
    all_shifts['name'] = np.where(all_shifts['name'] == "JOHN (JACK) ROSLOVIC", "JACK ROSLOVIC", all_shifts['name'])
    all_shifts['name'] = np.where(all_shifts['name'] == "ANTHONY-JOHN (AJ) GREER", "A.J. GREER", all_shifts['name'])
    all_shifts['name'] = np.where(all_shifts['name'] == 'MARTIN FEHARVARY', 'MARTIN FEHERVARY', all_shifts['name'])
    all_shifts['name'] = np.where(all_shifts['name'] == 'MATAJ  BLAMEL', 'MATAJ BLAMEL', all_shifts['name'])
    all_shifts['name'] = all_shifts['name'].str.replace('  ', ' ')

    return all_shifts


def _cap_times(all_shifts, live):
    """Cap invalid time values (>=20:00 or negative)."""
    try:
        all_shifts = all_shifts.reset_index(drop=True)

        for col in ['start_time', 'end_time']:
            time_parts = all_shifts[col].str.split(':', expand=True)
            if len(time_parts.columns) >= 2:
                minutes = pd.to_numeric(time_parts[0], errors='coerce')
                needs_cap = (minutes >= 20) & minutes.notna()
                needs_zero = (minutes < 0) & minutes.notna() & (not live)
                all_shifts.loc[needs_cap, col] = '20:00'
                if not live:
                    all_shifts.loc[needs_zero, col] = '0:00'
    except Exception as e:
        _log_exception_with_dataframe(e, 'shift_processing_api._cap_times', {
            'all_shifts': all_shifts
        })

    return all_shifts


def _assign_jersey_numbers(all_shifts, roster_cache):
    """
    Assign jersey numbers to shifts by matching normalized player names to roster.

    Args:
        all_shifts: DataFrame with 'name' column (normalized)
        roster_cache: Roster DataFrame with 'Name' and '#' columns
    """
    if '#' not in roster_cache.columns:
        return all_shifts

    # Build name → number mapping from roster
    number_map = {}
    for _, row in roster_cache.iterrows():
        name = str(row['Name']).strip().upper()
        num = str(row['#']).strip()
        number_map[name] = num

    all_shifts['number'] = all_shifts['name'].map(number_map).fillna(all_shifts['number'])

    # For any remaining unmapped, try fuzzy matching on last name
    unmapped = all_shifts[all_shifts['number'] == '']
    if len(unmapped) > 0:
        for idx, row in unmapped.iterrows():
            player_name = row['name']
            # Try matching by last name + team
            parts = player_name.split()
            if len(parts) >= 2:
                last_name = parts[-1]
                team_roster = roster_cache[
                    (roster_cache.team == row.get('venue', '')) &
                    (roster_cache.Name.str.contains(last_name, case=False, na=False))
                ]
                if len(team_roster) == 1:
                    all_shifts.at[idx, 'number'] = str(team_roster['#'].iloc[0]).strip()

    return all_shifts


def _build_change_events(all_shifts, game_id, goalie_names, live):
    """
    Convert individual player shifts into CHANGE events.

    Same logic as the HTML version, producing identical output format.
    """
    # Fix end_time > start_time issue and flag goalies
    all_shifts = all_shifts.assign(
        end_time=np.where(
            pd.to_datetime(all_shifts.start_time).dt.time > pd.to_datetime(all_shifts.end_time).dt.time,
            '20:00', all_shifts.end_time),
        goalie=np.where(all_shifts.name.isin(goalie_names), 1, 0))

    all_shifts = all_shifts.merge(
        all_shifts[all_shifts.goalie == 1].groupby(['team', 'period'])['name'].nunique().reset_index().rename(
            columns={'name': 'period_gs'}), how='left').fillna(0)

    # Fix goalies who showed up late in the period
    all_shifts = all_shifts.assign(
        period_shift_number=all_shifts.groupby(['period', 'name']).cumcount() + 1)

    all_shifts = all_shifts.assign(
        start_time=np.where(
            (all_shifts.goalie == 1) & (all_shifts.start_time != '0:00') &
            (all_shifts.period_gs == 1) & (all_shifts.period_shift_number == 1),
            '0:00', all_shifts.start_time))

    all_shifts.start_time = all_shifts.start_time.str.strip()
    all_shifts.end_time = all_shifts.end_time.str.strip()
    all_shifts['number'] = all_shifts.number.astype(str)

    changes_on = all_shifts.groupby(['team', 'period', 'start_time']).agg(
        on=('name', ', '.join),
        on_numbers=('number', ', '.join),
        number_on=('name', 'count')
    ).reset_index().rename(columns={'start_time': 'time'}).sort_values(by=['team', 'period', 'time'])

    changes_off = all_shifts.groupby(['team', 'period', 'end_time']).agg(
        off=('name', ', '.join),
        off_numbers=('number', ', '.join),
        number_off=('name', 'count')
    ).reset_index().rename(columns={'end_time': 'time'}).sort_values(by=['team', 'period', 'time'])

    all_on = changes_on.merge(changes_off, on=['team', 'period', 'time'], how='left')
    off_only = changes_off.merge(changes_on, on=['team', 'period', 'time'], how='left', indicator=True)[
        changes_off.merge(changes_on, on=['team', 'period', 'time'], how='left', indicator=True)['_merge'] != 'both']
    full_changes = pd.concat([all_on, off_only]).sort_values(by=['period', 'time']).drop(columns=['_merge'])

    full_changes['period_seconds'] = full_changes.time.str.split(':').str[0].astype(int) * 60 + \
                                     full_changes.time.str.split(':').str[1].astype(int)

    full_changes['game_seconds'] = (np.where(
        (full_changes.period < 5) & (int(game_id) != 3),
        (((full_changes.period - 1) * 1200) + full_changes.period_seconds),
        3900))

    full_changes = full_changes.assign(
        team=np.where(full_changes.team.str.contains('CANADI'), 'MONTREAL CANADIENS', full_changes.team)
    ).sort_values(by='game_seconds')

    return full_changes


def scrape_api_shifts(full_game_id, live=True, roster_cache=None, verbose=False):
    """
    Scrape shift data from the NHL REST API.

    Drop-in replacement for scrape_html_shifts() from shift_processing.py.

    Args:
        full_game_id: Full NHL game ID (e.g., 2025020420)
        live: Boolean flag for live games
        roster_cache: Roster cache DataFrame for goalie names and jersey numbers
        verbose: If True, print detailed timing information

    Returns:
        DataFrame with CHANGE events (or tuple of (min_game_clock, DataFrame) for live games)
    """
    game_id_str = str(full_game_id)
    small_id = game_id_str[5:]  # e.g., '020420'

    goalie_names = roster_cache[roster_cache.Pos == 'G'].Name.unique().tolist()

    # Fetch shifts from API
    shift_records = _fetch_shifts_from_api(full_game_id, verbose=verbose)

    # Convert to DataFrame
    all_shifts, home_team_name, away_team_name = _api_shifts_to_dataframe(
        shift_records, roster_cache, verbose=verbose)

    if len(all_shifts) == 0:
        raise IndexError('This game has no shift data.')

    # Filter to valid periods (1-4, skip period 5 which is shootout)
    all_shifts = all_shifts[all_shifts.period.isin([1, 2, 3, 4])].copy()

    if len(all_shifts) == 0:
        raise IndexError('This game has no shift data after filtering periods.')

    # Apply name normalization (must happen before jersey number assignment)
    all_shifts = _apply_name_normalization(all_shifts)

    # Assign jersey numbers from roster
    all_shifts = _assign_jersey_numbers(all_shifts, roster_cache)

    # Filter negative durations
    all_shifts = all_shifts[~all_shifts.duration.str.startswith('-')]

    # Cap times
    all_shifts = _cap_times(all_shifts, live)

    # Handle live game clock detection
    min_game_clock = None
    if live:
        # For live games, detect the current game clock from the latest shift end times
        # per team. The API should have shifts up to the current point in the game.
        try:
            latest_per_team = []
            for team in all_shifts.team.unique():
                team_shifts = all_shifts[all_shifts.team == team]
                max_period = team_shifts.period.max()
                period_shifts = team_shifts[team_shifts.period == max_period]

                # Get latest shift end time in this period
                end_seconds = period_shifts.end_time.apply(
                    lambda x: convert_clock_to_seconds(x) if ':' in str(x) else 0)
                if len(end_seconds) > 0:
                    latest_end = end_seconds.max()
                    latest_per_team.append({
                        'period': max_period,
                        'seconds': latest_end
                    })

            if latest_per_team:
                # Use the minimum of the latest shift ends as the game clock
                # (conservative: use the team that's furthest behind)
                min_item = min(latest_per_team,
                               key=lambda x: (x['period'] - 1) * 1200 + x['seconds'])
                min_game_clock = ((min_item['period'] - 1) * 1200) + min_item['seconds']
        except Exception as e:
            if verbose:
                print(f'  Warning: Could not determine live game clock: {e}')
            min_game_clock = None

    # Build CHANGE events
    full_changes = _build_change_events(all_shifts, small_id, goalie_names, live)

    if live:
        if min_game_clock is not None:
            full_changes = full_changes[full_changes.game_seconds <= min_game_clock]
        return min_game_clock, full_changes.reset_index(drop=True)

    return full_changes.reset_index(drop=True)
