"""
NHL API Events Scraper
This module implements scrape_api_events() using the NHL API play-by-play endpoint
to replace ESPN scraping functionality.
"""

import numpy as np
import pandas as pd
import requests
import json
import re
import unicodedata
import time
import os

# Use the same session pattern as the main scraper
_session = requests.Session()

from TopDownHockey_Scraper.name_corrections import NAME_CORRECTIONS, normalize_player_name

# Load packaged handedness data
_handedness_dict = {}
_handedness_api_cache = {}  # Cache for API lookups during session

def _load_handedness_data():
    """Load handedness data from packaged CSV file"""
    global _handedness_dict
    try:
        # Try importlib.resources first (Python 3.9+)
        try:
            from importlib.resources import files
            data_path = files('TopDownHockey_Scraper').joinpath('data', 'handedness.csv')
            with data_path.open('r') as f:
                df = pd.read_csv(f)
        except (ImportError, TypeError):
            # Fallback for older Python versions
            import pkg_resources
            data_path = pkg_resources.resource_filename('TopDownHockey_Scraper', 'data/handedness.csv')
            df = pd.read_csv(data_path)

        _handedness_dict = dict(zip(df['player'], df['handedness']))
    except Exception as e:
        # If data file not found, continue without it (API fallback will be used)
        _handedness_dict = {}

# Load on module import
_load_handedness_data()

def _get_handedness_from_api(player_id):
    """Fetch player handedness from NHL API (with session caching)"""
    if player_id is None:
        return None

    player_id_str = str(int(player_id))

    # Check session cache first
    if player_id_str in _handedness_api_cache:
        return _handedness_api_cache[player_id_str]

    try:
        url = f"https://api-web.nhle.com/v1/player/{player_id_str}/landing"
        response = _session.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        handedness = data.get('shootsCatches')
        _handedness_api_cache[player_id_str] = handedness
        return handedness
    except Exception:
        _handedness_api_cache[player_id_str] = None
        return None

def _get_player_name(player_id, player_mapping_dict):
    """Get player name from ID using player mapping dictionary from API"""
    if player_id is None:
        return None
    
    player_id_str = str(int(player_id)) if pd.notna(player_id) else None
    return player_mapping_dict.get(player_id_str, None)

def _map_event_type(type_desc_key, type_code=None):
    """Map NHL API event types to ESPN-style event codes"""
    # NHL API uses typeDescKey for event descriptions
    event_mapping = {
        'shot-on-goal': 'SHOT',
        'shot-blocked': 'BLOCK',
        'shot-missed': 'MISS',
        'goal': 'GOAL',
        'hit': 'HIT',
        'giveaway': 'GIVE',
        'takeaway': 'TAKE',
        'faceoff': 'FAC',
        'penalty': 'PENL',
        'stoppage': 'STOP',
        'period-start': 'PSTR',
        'period-end': 'PEND',
        'game-end': 'GEND',
    }
    
    # Handle typeDescKey (string)
    if isinstance(type_desc_key, str):
        type_lower = type_desc_key.lower()
        # Try exact match first
        if type_lower in event_mapping:
            return event_mapping[type_lower]
        # Try partial matches
        if 'shot' in type_lower and 'goal' in type_lower:
            return 'SHOT'
        elif 'shot' in type_lower and 'block' in type_lower:
            return 'BLOCK'
        elif 'shot' in type_lower and 'miss' in type_lower:
            return 'MISS'
        elif 'goal' in type_lower:
            return 'GOAL'
        elif 'hit' in type_lower:
            return 'HIT'
        elif 'giveaway' in type_lower or 'give' in type_lower:
            return 'GIVE'
        elif 'takeaway' in type_lower or 'take' in type_lower:
            return 'TAKE'
        elif 'faceoff' in type_lower or 'face-off' in type_lower:
            return 'FAC'
        elif 'penalty' in type_lower:
            return 'PENL'
        elif 'stop' in type_lower:
            return 'STOP'
    
    # Handle typeCode (numeric) as fallback
    if isinstance(type_code, int):
        # Common NHL API event type codes
        type_id_mapping = {
            502: 'GOAL',
            503: 'HIT',
            504: 'GIVE',  # Note: giveaway and takeaway may share codes, check details
            505: 'SHOT',
            506: 'BLOCK',
            507: 'MISS',
            508: 'TAKE',
        }
        if type_code in type_id_mapping:
            return type_id_mapping[type_code]
    
    return 'UNKNOWN'

def _extract_player_id_from_event(event_details, event_type):
    """Extract the primary player ID from event details based on event type"""
    if not event_details:
        return None
    
    # Map event types to their corresponding player ID fields (NHL API field names)
    # Note: For BLOCK events, ESPN shows the shooting player (whose shot was blocked), not the blocker
    player_id_fields = {
        'SHOT': ['shootingPlayerId', 'scoringPlayerId', 'playerId'],
        'GOAL': ['scoringPlayerId', 'shootingPlayerId', 'playerId'],
        'HIT': ['hittingPlayerId', 'playerId'],
        'BLOCK': ['shootingPlayerId', 'playerId'],  # ESPN shows shooter, not blocker
        'GIVE': ['playerId', 'committedByPlayerId'],
        'TAKE': ['playerId', 'takingPlayerId'],
        'MISS': ['shootingPlayerId', 'playerId'],
        'FAC': ['winningPlayerId', 'playerId'],
        'PENL': ['committedByPlayerId', 'playerId'],
    }
    
    # Try to find the appropriate player ID field
    fields_to_try = player_id_fields.get(event_type, ['playerId'])
    
    for field in fields_to_try:
        if field in event_details and event_details[field] is not None:
            player_id = event_details[field]
            # Ensure it's a valid ID (not 0 or empty)
            if player_id and player_id != 0:
                return player_id
    
    # Fallback: try common field names
    for common_field in ['playerId', 'player', 'id']:
        if common_field in event_details and event_details[common_field] is not None:
            player_id = event_details[common_field]
            if player_id and player_id != 0:
                return player_id
    
    return None

def scrape_api_events(game_id, drop_description=True, shift_to_espn=False, verbose=False):
    """
    Scrape event coordinates and data from NHL API play-by-play endpoint.
    
    This function replaces scrape_espn_events() by using the official NHL API.
    
    Parameters:
    -----------
    game_id : int
        NHL game ID (e.g., 2025020331)
    drop_description : bool, default True
        Whether to drop the description column from the output
    shift_to_espn : bool, default False
        If True, raises KeyError to trigger ESPN fallback (for compatibility)
    verbose : bool, default False
        If True, print detailed timing information
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with columns: coords_x, coords_y, event_player_1, event, 
        game_seconds, period, version, goalie_id, goalie_name 
        (and optionally description)
    """
    
    if shift_to_espn:
        raise KeyError("shift_to_espn=True requested, triggering ESPN fallback")
    
    # Fetch play-by-play data from NHL API
    api_url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    
    try:
        # TIME: Network request
        net_start = time.time()
        response = _session.get(api_url, timeout=30)
        net_duration = time.time() - net_start
        if verbose:
            print(f'  ⏱️ API events network request: {net_duration:.2f}s')
        
        response.raise_for_status()
        
        # TIME: JSON parsing
        parse_start = time.time()
        api_data = json.loads(response.content)
        player_mapping_df = pd.DataFrame(api_data['rosterSpots'])
        player_mapping_df = player_mapping_df.assign(player = (player_mapping_df['firstName'].apply(lambda x: x['default']) + ' ' + player_mapping_df['lastName'].apply(lambda x: x['default'])).str.upper(),
                      link = 'https://assets.nhle.com/mugs/nhl/latest/' + player_mapping_df['playerId'].astype(str) + '.png',
                      id = player_mapping_df['playerId']).loc[:, ['player', 'link', 'id']]

        # Disambiguate players with identical names using their NHL API IDs
        # ELIAS PETTERSSON (D) - defenseman, ID 8483678 - vs forward ELIAS PETTERSSON
        player_mapping_df['player'] = np.where(
            player_mapping_df['id'] == 8483678,
            'ELIAS PETTERSSON(D)',
            player_mapping_df['player']
        )

        player_mapping_df['player'] = np.where(
            player_mapping_df['id'] == 8480222,
            'SEBASTIAN AHO SWE',
            player_mapping_df['player']
        )

        # Create dictionary mapping player ID to name for fast lookup
        player_mapping_dict = dict(zip(player_mapping_df['id'].astype(str), player_mapping_df['player']))
        parse_duration = time.time() - parse_start
        if verbose:
            print(f'  ⏱️ API JSON parsing: {parse_duration:.2f}s')
    except Exception as e:
        raise KeyError(f"Failed to fetch NHL API data for game {game_id}: {e}")
    
    # Extract plays array
    if 'plays' not in api_data:
        raise KeyError(f"No 'plays' key found in NHL API response for game {game_id}")
    
    plays = api_data['plays']
    
    if not plays:
        # Return empty DataFrame with correct columns
        columns = ['coords_x', 'coords_y', 'event_player_1', 'event', 'game_seconds', 'period', 'version', 'goalie_id', 'goalie_name', 'miss_reason', 'shooter_handedness']
        if not drop_description:
            columns.append('description')
        return pd.DataFrame(columns=columns)

    # Parse plays into list of dictionaries
    events_list = []
    
    for play in plays:
        # Extract period information
        period_desc = play.get('periodDescriptor', {})
        period = period_desc.get('number', 1)
        
        # Extract time information
        time_in_period = play.get('timeInPeriod', '')
        time_remaining = play.get('timeRemaining', '')
        
        # Parse time string (format: "MM:SS")
        if time_in_period:
            try:
                time_parts = time_in_period.split(':')
                minutes = int(time_parts[0])
                seconds = int(time_parts[1])
            except (ValueError, IndexError):
                minutes, seconds = 0, 0
        else:
            minutes, seconds = 0, 0
        
        # Calculate game_seconds
        if period < 5:
            game_seconds = ((period - 1) * 1200) + (minutes * 60) + seconds
        else:
            game_seconds = 3900  # Overtime
        
        # Extract event type
        event_type_code = play.get('typeCode')
        event_type_desc = play.get('typeDescKey', '')
        
        # Map to ESPN-style event code
        event_code = _map_event_type(event_type_desc, event_type_code)
        
        # Extract coordinates from details
        details = play.get('details', {})
        coords_x = details.get('xCoord')
        coords_y = details.get('yCoord')
        
        # Handle None coordinates
        if coords_x is None or coords_y is None:
            # Skip events without coordinates (except faceoffs which can be at 0,0)
            if event_code != 'FAC':
                continue
            # Set faceoff coordinates to 0 if missing
            coords_x = coords_x if coords_x is not None else 0
            coords_y = coords_y if coords_y is not None else 0
        
        # Extract player ID and map to name
        player_id = _extract_player_id_from_event(details, event_code)
        player_name = _get_player_name(player_id, player_mapping_dict) if player_id else None
        
        # Extract goalie ID and map to name
        goalie_id = details.get('goalieInNetId')
        goalie_name = _get_player_name(goalie_id, player_mapping_dict) if goalie_id else None

        # Extract miss reason (only present for missed shots)
        miss_reason = details.get('reason')

        # Extract description
        description = play.get('description', {})
        if isinstance(description, dict):
            description = description.get('default', '')
        description = str(description) if description else ''
        
        # Only include events with coordinates (matching ESPN behavior)
        # For faceoffs, allow missing player names (they'll be handled in merge)
        if coords_x is not None and coords_y is not None:
            
            events_list.append({
                'coords_x': int(coords_x),
                'coords_y': int(coords_y),
                'event_player_1': player_name,
                'event': event_code,
                'game_seconds': game_seconds,
                'period': period,
                'description': description,
                'time_in_period': time_in_period,
                'player_id': player_id,
                'goalie_id': goalie_id,
                'goalie_name': goalie_name,
                'miss_reason': miss_reason,
            })
    
    if not events_list:
        # Return empty DataFrame with correct columns
        columns = ['coords_x', 'coords_y', 'event_player_1', 'event', 'game_seconds', 'period', 'version', 'goalie_id', 'goalie_name', 'miss_reason', 'shooter_handedness']
        if not drop_description:
            columns.append('description')
        return pd.DataFrame(columns=columns)

    # Convert to DataFrame
    events_df = pd.DataFrame(events_list)
    
    # Filter out events without player names (matching ESPN behavior)
    # ESPN filters: events must have coords AND player names
    events_df = events_df[events_df['event_player_1'].notna()]
    
    # Normalize player names
    events_df['event_player_1'] = events_df['event_player_1'].apply(normalize_player_name)
    events_df['goalie_name'] = events_df['goalie_name'].apply(normalize_player_name)
    
    # Filter again after normalization (in case normalization resulted in empty strings)
    events_df = events_df[events_df['event_player_1'] != '']

    # Add shooter handedness from packaged data, with API fallback for unknowns
    def get_handedness(row):
        # Try packaged data first (fast)
        player_name = row['event_player_1']
        if player_name in _handedness_dict:
            return _handedness_dict[player_name]
        # Fall back to NHL API for unknown players (slow, but cached)
        return _get_handedness_from_api(row.get('player_id'))

    events_df['shooter_handedness'] = events_df.apply(get_handedness, axis=1)

    # Calculate priority for sorting (matching ESPN function)
    events_df['priority'] = np.where(
        events_df['event'].isin(['TAKE', 'GIVE', 'MISS', 'HIT', 'SHOT', 'BLOCK']), 1,
        np.where(events_df['event'] == 'GOAL', 2,
        np.where(events_df['event'] == 'STOP', 3,
        np.where(events_df['event'] == 'DELPEN', 4,
        np.where(events_df['event'] == 'PENL', 5,
        np.where(events_df['event'] == 'CHANGE', 6,
        np.where(events_df['event'] == 'PEND', 7,
        np.where(events_df['event'] == 'GEND', 8,
        np.where(events_df['event'] == 'FAC', 9, 0)))))))))
    
    # Sort by period, game_seconds, event_player_1, priority
    events_df = events_df.sort_values(
        by=['period', 'game_seconds', 'event_player_1', 'priority']
    ).reset_index(drop=True)
    
    # Calculate version numbers for duplicate events (matching ESPN logic)
    events_df['version'] = 0
    
    # Version 1: same event, player, and time as previous
    events_df['version'] = np.where(
        (events_df['event'] == events_df['event'].shift()) &
        (events_df['event_player_1'] == events_df['event_player_1'].shift()) &
        (events_df['event_player_1'] != '') &
        (events_df['game_seconds'] == events_df['game_seconds'].shift()),
        1, events_df['version']
    )
    
    # Version 2: same event, player, and time as 2 rows ago
    events_df['version'] = np.where(
        (events_df['event'] == events_df['event'].shift(2)) &
        (events_df['event_player_1'] == events_df['event_player_1'].shift(2)) &
        (events_df['game_seconds'] == events_df['game_seconds'].shift(2)) &
        (events_df['event_player_1'] != '') &
        (~events_df['description'].str.contains('Penalty Shot', na=False)),
        2, events_df['version']
    )
    
    # Version 3: same event, player, and time as 3 rows ago
    events_df['version'] = np.where(
        (events_df['event'] == events_df['event'].shift(3)) &
        (events_df['event_player_1'] == events_df['event_player_1'].shift(3)) &
        (events_df['game_seconds'] == events_df['game_seconds'].shift(3)) &
        (events_df['event_player_1'] != ''),
        3, events_df['version']
    )
    
    # Clip coordinates to valid ranges (matching ESPN function)
    events_df['coords_x'] = np.where(events_df['coords_x'] > 99, 99, events_df['coords_x'])
    events_df['coords_y'] = np.where(events_df['coords_y'] < -42, -42, events_df['coords_y'])
    
    # Select final columns (matching ESPN column order)
    # player_id is kept for fallback merge when name matching fails
    final_columns = ['coords_x', 'coords_y', 'event_player_1', 'event', 'game_seconds', 'period', 'version', 'goalie_id', 'goalie_name', 'miss_reason', 'shooter_handedness', 'player_id']
    if not drop_description:
        final_columns.append('description')

    events_df = events_df[final_columns]
    
    return events_df

