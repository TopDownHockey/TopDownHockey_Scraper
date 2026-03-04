"""
HTML-based shift processing logic, extracted from TopDownHockey_NHL_Scraper.py.

This module handles scraping shift data from NHL HTML shift reports and converting
individual player shifts into CHANGE events for the play-by-play pipeline.
"""

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import re
from datetime import datetime

from TopDownHockey_Scraper.name_corrections import normalize_player_name

# Import helper functions from the main scraper module
from TopDownHockey_Scraper.TopDownHockey_NHL_Scraper import (
    convert_clock_to_seconds,
    convert_seconds_to_clock,
    subtract_from_twenty_minutes,
    _session,
    _CAPTAIN_A_PATTERN,
    _CAPTAIN_C_PATTERN,
    _log_exception_with_dataframe,
)


def parse_goaltender_summary(goalie_table):
    """Parse the goaltender summary table into a DataFrame."""

    rows = goalie_table.find_all('tr')

    goalie_data = []
    current_team = None

    for row in rows:
        cells = row.find_all('td')
        if not cells:
            continue

        # Check if this is a team header row (contains team name)
        first_cell_text = cells[0].get_text(strip=True)

        # Team header row - look for visitorsectionheading or homesectionheading
        if 'visitorsectionheading' in str(cells[0].get('class', [])) or \
           'homesectionheading' in str(cells[0].get('class', [])):
            # Extract team name
            if first_cell_text and first_cell_text not in ['TOI', 'GOALS-SHOTS AGAINST', 'EV', 'PP', 'SH', 'TOT', '1', '2', '3']:
                current_team = first_cell_text
                # Normalize Montreal team name (handles encoding issues from ISO-8859-1 vs UTF-8)
                if 'MONTR' in current_team and 'CANAD' in current_team:
                    current_team = 'MONTREAL CANADIENS'
            continue

        # Skip subheader rows (EV, PP, SH, etc.)
        if first_cell_text in ['EV', 'PP', 'SH', 'TOT', '']:
            continue

        # Skip TEAM TOTALS and spacer rows
        if 'TEAM TOTALS' in first_cell_text or first_cell_text == '\xa0':
            continue

        # This should be a goaltender data row
        # Check if it has position "G" in the second cell
        if len(cells) >= 11:
            cell_texts = [c.get_text(strip=True) for c in cells]

            # Goalie rows have: Number, "G", Name, EV, PP, SH, TOT, P1, P2, P3, TOT
            if len(cell_texts) >= 2 and cell_texts[1] == 'G':
                goalie_data.append({
                    'team': current_team,
                    'number': cell_texts[0],
                    'name': cell_texts[2],
                    'EV Total': cell_texts[3] if cell_texts[3] else None,
                    'PP Total': cell_texts[4] if cell_texts[4] else None,
                    'TOI': cell_texts[6] if cell_texts[6] else None,
                })

    return pd.DataFrame(goalie_data)


def backfill_missing_goalie_shifts_from_period_summary(shifts_df, soup, goalie_names, team_name, venue):
    """
    Backfill missing goalie shifts for historical games where the NHL shift data
    has gaps. This parses the per-period summary table and creates synthetic shifts
    for periods where a goalie has TOI but no individual shifts recorded.

    This addresses a known NHL data quality issue in historical games (pre-2023-24)
    where goalie shifts are sometimes missing from the detailed shift list but
    present in the period summary.

    Args:
        shifts_df: DataFrame of individual shifts already parsed
        soup: BeautifulSoup object of the HTML shifts page
        goalie_names: List of goalie names for this team
        team_name: Team name string
        venue: 'home' or 'away'

    Returns:
        Updated shifts_df with backfilled goalie shifts
    """
    if len(shifts_df) == 0:
        return shifts_df

    # Parse the per-period summary data (class 'bborder + lborder +')
    found = soup.find_all('td', {'class':['playerHeading + border', 'bborder + lborder +']})
    if len(found) == 0:
        return shifts_df

    players = dict()
    current_player = None

    for i in range(len(found)):
        line = found[i].get_text()
        if line == '25 PETTERSSON, ELIAS':
            line = '25 PETTERSSON(D), ELIAS'
        if ', ' in line:
            # Player header row
            name_parts = line.split(',')
            if len(name_parts) >= 2:
                number_last = name_parts[0].split(' ', 1)
                number = number_last[0].strip()
                last_name = number_last[1].strip() if len(number_last) > 1 else ''
                first_name = name_parts[1].strip()
                full_name = first_name + " " + last_name
                players[full_name] = {
                    'number': number,
                    'name': full_name,
                    'shifts': []
                }
                current_player = full_name
        elif current_player is not None:
            players[current_player]['shifts'].append(line)

    # Build period summary dataframe
    period_summary_list = []
    for key in players.keys():
        shifts_array = players[key]['shifts']
        # Per-period summary has 6 columns: period, shifts, avg, TOI, EV Total, PP Total
        length = int(len(shifts_array) / 6)
        if length > 0:
            try:
                df = pd.DataFrame(np.array(shifts_array).reshape(length, 6)).rename(
                    columns={0: 'period', 1: 'shifts_count', 2: 'avg', 3: 'TOI', 4: 'EV Total', 5: 'PP Total'})
                df = df.assign(name=players[key]['name'], number=players[key]['number'])
                period_summary_list.append(df)
            except:
                continue

    if not period_summary_list:
        return shifts_df

    period_summary = pd.concat(period_summary_list, ignore_index=True)

    # Normalize goalie names for comparison
    period_summary['name_normalized'] = period_summary.name.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
    goalie_names_normalized = [n.upper() for n in goalie_names]

    # Filter to goalies only
    goalie_summary = period_summary[period_summary.name_normalized.isin(goalie_names_normalized)].copy()

    if len(goalie_summary) == 0:
        return shifts_df

    # Filter out TOT row and rows with no TOI
    goalie_summary = goalie_summary[
        (goalie_summary.period != 'TOT') &
        (goalie_summary.TOI.notna()) &
        (goalie_summary.TOI != '') &
        (goalie_summary.TOI != '\xa0')
    ].copy()

    if len(goalie_summary) == 0:
        return shifts_df

    # Convert period to int for comparison
    goalie_summary['period_int'] = goalie_summary.period.replace('OT', '4').astype(int)

    # Check which periods are missing individual shifts for each goalie
    shifts_to_add = []

    for _, row in goalie_summary.iterrows():
        goalie_name = row['name_normalized']
        period = row['period_int']
        toi = row['TOI']
        number = row['number']

        # Check if this goalie has any individual shifts for this period
        existing_shifts = shifts_df[
            (shifts_df.name.str.upper() == goalie_name) &
            (shifts_df.period.astype(str).replace('OT', '4').astype(int) == period)
        ]

        if len(existing_shifts) == 0 and toi and toi not in ['', '\xa0']:
            # No shifts exist but summary shows TOI - need to backfill
            try:
                toi_seconds = convert_clock_to_seconds(toi)
                if toi_seconds > 0:
                    # Get max shift number for this player
                    existing_player_shifts = shifts_df[shifts_df.name.str.upper() == goalie_name]
                    if len(existing_player_shifts) > 0:
                        max_shift_num = existing_player_shifts.shift_number.astype(int).max()
                    else:
                        max_shift_num = 0

                    # Create synthetic shift covering the full period TOI
                    # For simplicity, assume shift starts at 0:00 and ends at TOI
                    period_str = 'OT' if period == 4 else str(period)
                    shift_start = '0:00 / 20:00'
                    shift_end = f'{toi} / {subtract_from_twenty_minutes(toi)}'

                    shifts_to_add.append({
                        'shift_number': max_shift_num + 1,
                        'period': period_str,
                        'shift_start': shift_start,
                        'shift_end': shift_end,
                        'duration': toi,
                        'name': row['name'],
                        'number': number,
                        'team': team_name,
                        'venue': venue
                    })
            except:
                continue

    if shifts_to_add:
        new_shifts_df = pd.DataFrame(shifts_to_add)
        # Ensure column types match
        new_shifts_df['shift_number'] = new_shifts_df['shift_number'].astype(int)
        shifts_df = pd.concat([shifts_df, new_shifts_df], ignore_index=True)
        shifts_df = shifts_df.sort_values(by=['number', 'period', 'shift_number']).reset_index(drop=True)

    return shifts_df


def _parse_html_shift_page(soup, venue, goalie_names):
    """
    Parse an HTML shift page (home or away) and return individual shifts DataFrame.

    Args:
        soup: BeautifulSoup object of the shift page
        venue: 'home' or 'away'
        goalie_names: List of goalie names for this team

    Returns:
        Tuple of (shifts_df, team_name)
    """
    found = soup.find_all('td', {'class': ['playerHeading + border', 'lborder + bborder']})
    if len(found) == 0:
        raise IndexError('This game has no shift data.')
    thisteam = soup.find('td', {'align': 'center', 'class': 'teamHeading + border'}).get_text()
    # Normalize Montreal team name (handles encoding issues)
    if 'MONTR' in thisteam and 'CANAD' in thisteam:
        thisteam = 'MONTREAL CANADIENS'

    players = dict()
    full_name = None

    for i in range(len(found)):
        line = found[i].get_text()
        if line == '25 PETTERSSON, ELIAS':
            line = '25 PETTERSSON(D), ELIAS'
        if ', ' in line:
            name_parts = line.split(',')
            if len(name_parts) >= 2:
                number_last = name_parts[0].split(' ', 1)
                number = number_last[0].strip()
                last_name = number_last[1].strip() if len(number_last) > 1 else ''
                first_name = name_parts[1].strip()
                full_name = first_name + " " + last_name
                players[full_name] = {
                    'number': number,
                    'name': full_name,
                    'shifts': []
                }
        else:
            if full_name is not None:
                players[full_name]['shifts'].append(line)

    alldf_list = []
    for key in players.keys():
        shifts_array = np.array(players[key]['shifts'])
        length = (len(shifts_array) // 5) * 5
        shifts_array = shifts_array[:length]
        df = pd.DataFrame(shifts_array.reshape(-1, 5)).rename(
            columns={0: 'shift_number', 1: 'period', 2: 'shift_start', 3: 'shift_end', 4: 'duration'})
        df = df.assign(name=players[key]['name'],
                       number=players[key]['number'],
                       team=thisteam,
                       venue=venue)
        alldf_list.append(df)

    shifts_df = pd.concat(alldf_list, ignore_index=True) if alldf_list else pd.DataFrame()
    return shifts_df, thisteam


def _parse_period_summary(soup, venue):
    """
    Parse the per-period summary section of an HTML shift page.

    Args:
        soup: BeautifulSoup object of the shift page
        venue: 'home' or 'away'

    Returns:
        Tuple of (extra_shifts_df, team_name)
    """
    found = soup.find_all('td', {'class': ['playerHeading + border', 'bborder + lborder +']})
    if len(found) == 0:
        raise IndexError('This game has no shift data.')
    thisteam = soup.find('td', {'align': 'center', 'class': 'teamHeading + border'}).get_text()
    if 'MONTR' in thisteam and 'CANAD' in thisteam:
        thisteam = 'MONTREAL CANADIENS'

    players = dict()
    full_name = None

    for i in range(len(found)):
        line = found[i].get_text()
        if line == '25 PETTERSSON, ELIAS':
            line = '25 PETTERSSON(D), ELIAS'
        if ', ' in line:
            name_parts = line.split(',')
            if len(name_parts) >= 2:
                number_last = name_parts[0].split(' ', 1)
                number = number_last[0].strip()
                last_name = number_last[1].strip() if len(number_last) > 1 else ''
                first_name = name_parts[1].strip()
                full_name = first_name + " " + last_name
                players[full_name] = {
                    'number': number,
                    'name': full_name,
                    'shifts': []
                }
        else:
            if full_name is not None:
                players[full_name]['shifts'].append(line)

    alldf_list = []
    for key in players.keys():
        length = int(len(players[key]['shifts']) / 6)
        df = pd.DataFrame(np.array((players[key]['shifts'])).reshape(length, 6)).rename(
            columns={0: 'period', 1: 'shifts', 2: 'avg', 3: 'TOI', 4: 'EV Total', 5: 'PP Total'})
        df = df.assign(name=players[key]['name'],
                       number=players[key]['number'],
                       team=thisteam,
                       venue=venue)
        alldf_list.append(df)

    extra_shifts = pd.concat(alldf_list, ignore_index=True) if alldf_list else pd.DataFrame()
    return extra_shifts, thisteam


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
        _log_exception_with_dataframe(e, 'shift_processing._cap_times', {
            'all_shifts': all_shifts
        })

    return all_shifts


def _build_change_events(all_shifts, game_id, goalie_names, live):
    """
    Convert individual player shifts into CHANGE events.

    Args:
        all_shifts: DataFrame of individual shifts with start_time, end_time, name, etc.
        game_id: Game ID string (small format, e.g., '020333')
        goalie_names: List of goalie names
        live: Boolean flag for live games

    Returns:
        DataFrame of CHANGE events with on/off player info and game_seconds
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


def scrape_html_shifts(season, game_id, live=True, home_page=None, away_page=None,
                       summary=None, roster_cache=None, verbose=False):
    """
    Scrape HTML shifts pages.

    Args:
        season: Season string (e.g., '20242025')
        game_id: Game ID string (e.g., '020333')
        live: Boolean flag for live games
        home_page: Optional pre-fetched requests.Response object for home shifts page
        away_page: Optional pre-fetched requests.Response object for away shifts page
        summary: Optional summary page for goalie data
        roster_cache: Roster cache for goalie names
        verbose: If True, print detailed timing information

    Returns:
        DataFrame with shift information (or tuple of (min_game_clock, DataFrame) for live games)
    """
    goalie_names = roster_cache[roster_cache.Pos == 'G'].Name.unique().tolist()
    home_goalie_names = roster_cache[(roster_cache.Pos == 'G') & (roster_cache.team == 'home')].Name.unique().tolist()
    away_goalie_names = roster_cache[(roster_cache.Pos == 'G') & (roster_cache.team == 'away')].Name.unique().tolist()

    # Fetch pages if not provided
    if home_page is None:
        url = 'http://www.nhl.com/scores/htmlreports/' + season + '/TH0' + game_id + '.HTM'
        home_page = _session.get(url, timeout=10)

    if away_page is None:
        url = 'http://www.nhl.com/scores/htmlreports/' + season + '/TV0' + game_id + '.HTM'
        away_page = _session.get(url, timeout=10)

    # Parse HTML
    if type(home_page) == str:
        home_soup = BeautifulSoup(home_page)
    else:
        home_soup = BeautifulSoup(home_page.text, 'lxml')

    if type(away_page) == str:
        away_soup = BeautifulSoup(away_page)
    else:
        away_soup = BeautifulSoup(away_page.text, 'lxml')

    # Parse individual shifts
    home_shifts, home_team_name = _parse_html_shift_page(home_soup, 'home', home_goalie_names)
    away_shifts, away_team_name = _parse_html_shift_page(away_soup, 'away', away_goalie_names)

    home_clock_period = None
    home_clock_time_now = None
    away_clock_period = None
    away_clock_time_now = None

    # Live game gap-filling
    if live:
        home_shifts = home_shifts.assign(shift_number=home_shifts.shift_number.astype(int))
        home_shifts = home_shifts.assign(number=home_shifts.number.astype(int))

        home_extra_shifts, _ = _parse_period_summary(home_soup, 'home')

        # Check if goalie is missing for current period
        if len(home_shifts[(home_shifts.period == max(home_shifts.period)) & (home_shifts.name.isin(home_goalie_names))]) == 0 and \
           len(home_extra_shifts[home_extra_shifts.name.isin(home_goalie_names)]) == 0:
            if type(summary) == str:
                summary_soup = BeautifulSoup(summary)
            else:
                summary_soup = BeautifulSoup(summary.content.decode('ISO-8859-1'))

            sections = summary_soup.find_all('td', class_='sectionheading')
            for section in sections:
                if 'GOALTENDER SUMMARY' in section.get_text():
                    goalie_table = section.find_parent('tr').find_next_sibling('tr').find('table')
                    break

            goalie_summary = parse_goaltender_summary(goalie_table)
            goalie_summary = goalie_summary[
                ((goalie_summary.team == home_team_name) |
                 (('CANADIENS' in home_team_name) & (goalie_summary.team.str.contains('CANADIENS')))) &
                ~(pd.isna(goalie_summary['TOI']))]

            goalie_summary = goalie_summary.assign(
                name=goalie_summary.name.str.split(', ').str[-1] + ' ' + goalie_summary.name.str.split(', ').str[0])
            goalie_summary.name = goalie_summary.name.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
            goalie_summary = goalie_summary.assign(
                period=max(home_shifts.period), shifts='1', avg=goalie_summary.TOI, venue='home'
            ).loc[:, home_extra_shifts.columns]
            home_extra_shifts = pd.concat([home_extra_shifts, goalie_summary])

        home_extra_shifts = home_extra_shifts.assign(
            TOI_seconds_summary=home_extra_shifts.TOI.apply(lambda x: convert_clock_to_seconds(x)))
        home_extra_shifts = home_extra_shifts.merge(
            home_shifts.assign(toi_secs=home_shifts.duration.apply(lambda x: convert_clock_to_seconds(x))
            ).groupby(['name', 'period'])['toi_secs'].sum().reset_index(),
            how='left').fillna(0)
        home_extra_shifts['toi_secs'] = home_extra_shifts['toi_secs'].astype(int)
        home_extra_shifts = home_extra_shifts.assign(
            toi_diff=abs(home_extra_shifts.toi_secs - home_extra_shifts.TOI_seconds_summary))

        shifts_needing_to_be_added = home_extra_shifts[home_extra_shifts.toi_diff != 0]

        if len(shifts_needing_to_be_added) > 0:
            latest_shift_end = home_shifts.assign(
                period_secs=home_shifts.shift_end.str.split(' / ').str[0].apply(lambda x: convert_clock_to_seconds(x))
            )[home_shifts.period == max(home_shifts.period)].sort_values(
                by='period_secs', ascending=False).period_secs.iloc[0]

            max_toi = shifts_needing_to_be_added.TOI.apply(lambda x: convert_clock_to_seconds(x)).max()
            overage = max_toi - latest_shift_end
            if overage > 0:
                shifts_needing_to_be_added.toi_diff = shifts_needing_to_be_added.toi_diff - overage

            home_clock_time_now = convert_seconds_to_clock(latest_shift_end)
            home_clock_period = max(home_shifts.period.replace('OT', 4).astype(int))

            shifts_needing_to_be_added = shifts_needing_to_be_added.assign(
                shift_start=((convert_clock_to_seconds(home_clock_time_now) - shifts_needing_to_be_added.toi_diff).apply(
                    lambda x: convert_seconds_to_clock(x)).astype(str)
                    + ' / ' + (convert_clock_to_seconds(home_clock_time_now) - shifts_needing_to_be_added.toi_diff).apply(
                    lambda x: convert_seconds_to_clock(x)).astype(str).apply(lambda x: subtract_from_twenty_minutes(x))),
                shift_end=home_clock_time_now + ' / ' + subtract_from_twenty_minutes(home_clock_time_now),
                duration=shifts_needing_to_be_added.toi_diff)

            shifts_needing_to_be_added = shifts_needing_to_be_added.assign(
                duration=shifts_needing_to_be_added.toi_diff.apply(lambda x: convert_seconds_to_clock(x)))

            shifts_needing_to_be_added = shifts_needing_to_be_added.merge(
                home_shifts.assign(shift_number=home_shifts.shift_number.astype(int)).groupby('name')['shift_number'].max().reset_index().rename(
                    columns={'shift_number': 'prior_max_shift'}),
                how='left').fillna(0)

            shifts_needing_to_be_added = shifts_needing_to_be_added.assign(
                shift_number=shifts_needing_to_be_added.prior_max_shift + 1)
            shifts_needing_to_be_added.shift_number = shifts_needing_to_be_added.shift_number.astype(int)
            shifts_needing_to_be_added = shifts_needing_to_be_added.loc[
                :, ['shift_number', 'period', 'shift_start', 'shift_end', 'duration', 'name', 'number', 'team', 'venue']]
            shifts_needing_to_be_added['number'] = shifts_needing_to_be_added['number'].astype(int)
            home_shifts = pd.concat([home_shifts, shifts_needing_to_be_added]).sort_values(
                by=['number', 'period', 'shift_number'])
        else:
            home_clock_period = None
            home_clock_time_now = None

        # Away live gap-filling
        away_shifts = away_shifts.assign(shift_number=away_shifts.shift_number.astype(int))
        away_shifts = away_shifts.assign(number=away_shifts.number.astype(int))

        away_extra_shifts, _ = _parse_period_summary(away_soup, 'away')

        if len(away_shifts[(away_shifts.period == max(away_shifts.period)) & (away_shifts.name.isin(away_goalie_names))]) == 0 and \
           len(away_extra_shifts[away_extra_shifts.name.isin(away_goalie_names)]) == 0:
            if type(summary) == str:
                summary_soup = BeautifulSoup(summary)
            else:
                summary_soup = BeautifulSoup(summary.content.decode('ISO-8859-1'))

            sections = summary_soup.find_all('td', class_='sectionheading')
            for section in sections:
                if 'GOALTENDER SUMMARY' in section.get_text():
                    goalie_table = section.find_parent('tr').find_next_sibling('tr').find('table')
                    break

            goalie_summary = parse_goaltender_summary(goalie_table)
            goalie_summary = goalie_summary[
                ((goalie_summary.team == away_team_name) |
                 (('CANADIENS' in away_team_name) & (goalie_summary.team.str.contains('CANADIENS')))) &
                ~(pd.isna(goalie_summary['TOI']))]

            goalie_summary = goalie_summary.assign(
                name=goalie_summary.name.str.split(', ').str[-1] + ' ' + goalie_summary.name.str.split(', ').str[0])
            goalie_summary.name = goalie_summary.name.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
            goalie_summary = goalie_summary.assign(
                period=max(away_shifts.period), shifts='1', avg=goalie_summary.TOI, venue='away'
            ).loc[:, away_extra_shifts.columns]
            away_extra_shifts = pd.concat([away_extra_shifts, goalie_summary])

        away_extra_shifts = away_extra_shifts.assign(
            TOI_seconds_summary=away_extra_shifts.TOI.apply(lambda x: convert_clock_to_seconds(x)))
        away_extra_shifts = away_extra_shifts.merge(
            away_shifts.assign(toi_secs=away_shifts.duration.apply(lambda x: convert_clock_to_seconds(x))
            ).groupby(['name', 'period'])['toi_secs'].sum().reset_index(),
            how='left').fillna(0)
        away_extra_shifts['toi_secs'] = away_extra_shifts['toi_secs'].astype(int)
        away_extra_shifts = away_extra_shifts.assign(
            toi_diff=abs(away_extra_shifts.toi_secs - away_extra_shifts.TOI_seconds_summary))

        shifts_needing_to_be_added = away_extra_shifts[away_extra_shifts.toi_diff != 0]

        if len(shifts_needing_to_be_added) > 0:
            latest_shift_end = away_shifts.assign(
                period_secs=away_shifts.shift_end.str.split(' / ').str[0].apply(lambda x: convert_clock_to_seconds(x))
            )[away_shifts.period == max(away_shifts.period)].sort_values(
                by='period_secs', ascending=False).period_secs.iloc[0]

            max_toi = shifts_needing_to_be_added.TOI.apply(lambda x: convert_clock_to_seconds(x)).max()
            overage = max_toi - latest_shift_end
            if overage > 0:
                shifts_needing_to_be_added.toi_diff = shifts_needing_to_be_added.toi_diff - overage

            away_clock_time_now = convert_seconds_to_clock(latest_shift_end)
            away_clock_period = max(away_shifts.period.replace('OT', 4).astype(int))

            shifts_needing_to_be_added = shifts_needing_to_be_added.assign(
                shift_start=((convert_clock_to_seconds(away_clock_time_now) - shifts_needing_to_be_added.toi_diff).apply(
                    lambda x: convert_seconds_to_clock(x)).astype(str)
                    + ' / ' + (convert_clock_to_seconds(away_clock_time_now) - shifts_needing_to_be_added.toi_diff).apply(
                    lambda x: convert_seconds_to_clock(x)).astype(str).apply(lambda x: subtract_from_twenty_minutes(x))),
                shift_end=away_clock_time_now + ' / ' + subtract_from_twenty_minutes(away_clock_time_now),
                duration=shifts_needing_to_be_added.toi_diff)

            shifts_needing_to_be_added = shifts_needing_to_be_added.assign(
                duration=shifts_needing_to_be_added.toi_diff.apply(lambda x: convert_seconds_to_clock(x)))

            shifts_needing_to_be_added = shifts_needing_to_be_added.merge(
                away_shifts.assign(shift_number=away_shifts.shift_number.astype(int)).groupby('name')['shift_number'].max().reset_index().rename(
                    columns={'shift_number': 'prior_max_shift'}),
                how='left').fillna(0)

            shifts_needing_to_be_added = shifts_needing_to_be_added.assign(
                shift_number=shifts_needing_to_be_added.prior_max_shift + 1)
            shifts_needing_to_be_added.shift_number = shifts_needing_to_be_added.shift_number.astype(int)
            shifts_needing_to_be_added = shifts_needing_to_be_added.loc[
                :, ['shift_number', 'period', 'shift_start', 'shift_end', 'duration', 'name', 'number', 'team', 'venue']]
            shifts_needing_to_be_added['number'] = shifts_needing_to_be_added['number'].astype(int)
            away_shifts = pd.concat([away_shifts, shifts_needing_to_be_added]).sort_values(
                by=['number', 'period', 'shift_number'])
        else:
            away_clock_period = None
            away_clock_time_now = None

    # Backfill missing goalie shifts for historical seasons (pre-2023-24)
    if not live and int(season) < 20232024:
        home_shifts = backfill_missing_goalie_shifts_from_period_summary(
            home_shifts, home_soup, home_goalie_names, home_team_name, 'home')
        away_shifts = backfill_missing_goalie_shifts_from_period_summary(
            away_shifts, away_soup, away_goalie_names, away_team_name, 'away')

    # Filter negative durations
    home_shifts = home_shifts[~home_shifts.duration.str.startswith('-')]
    away_shifts = away_shifts[~away_shifts.duration.str.startswith('-')]

    # Combine
    all_shifts = pd.concat([home_shifts, away_shifts])

    # Name normalization
    all_shifts.name = all_shifts.name.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()

    # Extract times
    all_shifts = all_shifts.assign(start_time=all_shifts.shift_start.str.split('/').str[0])
    all_shifts = all_shifts.assign(end_time=all_shifts.shift_end.str.split('/').str[0])

    # Filter to valid periods
    if len(all_shifts) > 0:
        period_str = all_shifts.period.astype(str).str.strip()
        valid_mask = period_str.isin(['1', '2', '3', '4', 'OT'])
        all_shifts = all_shifts[valid_mask].copy()

        if len(all_shifts) > 0:
            all_shifts.period = (np.where(all_shifts.period == 'OT', 4, all_shifts.period)).astype(int)

    # Handle missing end times (non-breaking space means shift hasn't ended)
    all_shifts = all_shifts.assign(end_time=np.where(
        ~all_shifts.shift_end.str.contains('\xa0'), all_shifts.end_time,
        (np.where(
            (((pd.to_datetime(((60 * (all_shifts.start_time.str.split(':').str[0].astype(int))) +
            (all_shifts.start_time.str.split(':').str[1].astype(int)) +
            (60 * (all_shifts.duration.str.split(':').str[0].astype(int))).astype(int) +
            (all_shifts.duration.str.split(':').str[1].astype(int))).astype(int), unit='s'))).dt.time).astype(str).str[3:].str[0] == '0',
            (((pd.to_datetime(((60 * (all_shifts.start_time.str.split(':').str[0].astype(int))) +
            (all_shifts.start_time.str.split(':').str[1].astype(int)) +
            (60 * (all_shifts.duration.str.split(':').str[0].astype(int))).astype(int) +
            (all_shifts.duration.str.split(':').str[1].astype(int))).astype(int), unit='s'))).dt.time).astype(str).str[4:],
            (((pd.to_datetime(((60 * (all_shifts.start_time.str.split(':').str[0].astype(int))) +
            (all_shifts.start_time.str.split(':').str[1].astype(int)) +
            (60 * (all_shifts.duration.str.split(':').str[0].astype(int))).astype(int) +
            (all_shifts.duration.str.split(':').str[1].astype(int))).astype(int), unit='s'))).dt.time).astype(str).str[4:]))))

    # Apply name normalization
    all_shifts = _apply_name_normalization(all_shifts)

    # Cap times
    all_shifts = _cap_times(all_shifts, live)

    # Build CHANGE events
    full_changes = _build_change_events(all_shifts, game_id, goalie_names, live)

    if live:
        if home_clock_period is not None and away_clock_period is not None:
            min_game_clock = ((min([home_clock_period, away_clock_period]) - 1) * 1200) + \
                             min([convert_clock_to_seconds(home_clock_time_now), convert_clock_to_seconds(away_clock_time_now)])
        elif home_clock_period is not None:
            min_game_clock = ((home_clock_period - 1) * 1200) + convert_clock_to_seconds(home_clock_time_now)
        elif away_clock_period is not None:
            min_game_clock = ((away_clock_period - 1) * 1200) + convert_clock_to_seconds(away_clock_time_now)
        else:
            min_game_clock = None

        if min_game_clock is not None:
            full_changes = full_changes[full_changes.game_seconds <= min_game_clock]

        return min_game_clock, full_changes.reset_index(drop=True)

    return full_changes.reset_index(drop=True)
