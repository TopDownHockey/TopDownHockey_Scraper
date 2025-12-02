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

# Use the same session pattern as the main scraper
_session = requests.Session()

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
        'BLOCK': ['shootingPlayerId', 'blockingPlayerId', 'playerId'],  # ESPN shows shooter, not blocker
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

def _normalize_player_name(name):
    """Apply the same name normalization as scrape_espn_events"""
    if pd.isna(name) or name == '':
        return name
    
    name = str(name).strip()
    
    # Remove (A) and (C) designations
    name = re.sub(r' \(A\)$', '', name).strip()
    name = re.sub(r' \(C\)$', '', name).strip()
    
    # Normalize unicode characters
    name = unicodedata.normalize('NFKD', name).encode('ascii', errors='ignore').decode('utf-8')
    name = name.upper()
    
    # Common name replacements
    name = name.replace('ALEXANDRE ', 'ALEX ')
    name = name.replace('ALEXANDER ', 'ALEX ')
    name = name.replace('CHRISTOPHER ', 'CHRIS ')
    
    _NAME_CORRECTIONS = {
        "ANDREI KASTSITSYN": "ANDREI KOSTITSYN",
        "AJ GREER": "A.J. GREER",
        "ANDREW GREENE": "ANDY GREENE",
        "ANDREW WOZNIEWSKI": "ANDY WOZNIEWSKI",
        "ANTHONY DEANGELO": "TONY DEANGELO",
        "BATES (JON) BATTAGLIA": "BATES BATTAGLIA",
        "BRADLEY MILLS": "BRAD MILLS",
        "CAMERON BARKER": "CAM BARKER",
        "COLIN (JOHN) WHITE": "COLIN WHITE",
        "CRISTOVAL NIEVES": "BOO NIEVES",
        "CHRIS VANDE VELDE": "CHRIS VANDEVELDE",
        "DANNY BRIERE": "DANIEL BRIERE",
        "DANIEL GIRARDI": "DAN GIRARDI",
        "DANNY O'REGAN": "DANIEL O'REGAN",
        "DANIEL CARCILLO": "DAN CARCILLO",
        "DAVID JOHNNY ODUYA": "JOHNNY ODUYA",
        "DAVID BOLLAND": "DAVE BOLLAND",
        "DENIS JR. GAUTHIER": "DENIS GAUTHIER",
        "DWAYNE KING": "DJ KING",
        "EDWARD PURCELL": "TEDDY PURCELL",
        "EMMANUEL FERNANDEZ": "MANNY FERNANDEZ",
        "EMMANUEL LEGACE": "MANNY LEGACE",
        "EVGENII DADONOV": "EVGENY DADONOV",
        "FREDDY MODIN": "FREDRIK MODIN",
        "FREDERICK MEYER IV": "FREDDY MEYER",
        "HARRISON ZOLNIERCZYK": "HARRY ZOLNIERCZYK",
        "ILJA BRYZGALOV": "ILYA BRYZGALOV",
        "JACOB DOWELL": "JAKE DOWELL",
        "JAMES HOWARD": "JIMMY HOWARD",
        "JAMES VANDERMEER": "JIM VANDERMEER",
        "JAMES WYMAN": "JT WYMAN",
        "JOHN HILLEN III": "JACK HILLEN",
        "JOHN ODUYA": "JOHNNY ODUYA",
        "JOHN PEVERLEY": "RICH PEVERLEY",
        "JONATHAN SIM": "JON SIM",
        "JONATHON KALINSKI": "JON KALINSKI",
        "JONATHAN AUDY-MARCHESSAULT": "JONATHAN MARCHESSAULT",
        "JOSEPH CRABB": "JOEY CRABB",
        "JOSEPH CORVO": "JOE CORVO",
        "JOSHUA BAILEY": "JOSH BAILEY",
        "JOSHUA HENNESSY": "JOSH HENNESSY",
        "JOSHUA MORRISSEY": "JOSH MORRISSEY",
        "JEAN-FRANCOIS JACQUES": "J-F JACQUES",
        "JT COMPHER": "J.T. COMPHER",
        "KRISTOPHER LETANG": "KRIS LETANG",
        "KRYSTOFER BARCH": "KRYS BARCH",
        "KRYSTOFER KOLANOS": "KRYS KOLANOS",
        "MARC POULIOT": "MARC-ANTOINE POULIOT",
        "MARTIN ST LOUIS": "MARTIN ST. LOUIS",
        "MARTIN ST PIERRE": "MARTIN ST. PIERRE",
        "MARTY HAVLAT": "MARTIN HAVLAT",
        "MATTHEW CARLE": "MATT CARLE",
        "MATHEW DUMBA": "MATT DUMBA",
        "MATTHEW BENNING": "MATT BENNING",
        "MATTHEW IRWIN": "MATT IRWIN",
        "MATTHEW NIETO": "MATT NIETO",
        "MATTHEW STAJAN": "MATT STAJAN",
        "MAXIM MAYOROV": "MAKSIM MAYOROV",
        "MAXIME TALBOT": "MAX TALBOT",
        "MAXWELL REINHART": "MAX REINHART",
        "MICHAEL BLUNDEN": "MIKE BLUNDEN",
        "MICHAEL CAMMALLERI": "MIKE CAMMALLERI",
        "MICHAEL FERLAND": "MICHEAL FERLAND",
        "MICHAEL GRIER": "MIKE GRIER",
        "MICHAEL KNUBLE": "MIKE KNUBLE",
        "MICHAEL KOMISAREK": "MIKE KOMISAREK",
        "MICHAEL MATHESON": "MIKE MATHESON",
        "MICHAEL MODANO": "MIKE MODANO",
        "MICHAEL RUPP": "MIKE RUPP",
        "MICHAEL SANTORELLI": "MIKE SANTORELLI",
        "MICHAEL SILLINGER": "MIKE SILLINGER",
        "MITCHELL MARNER": "MITCH MARNER",
        "NATHAN GUENIN": "NATE GUENIN",
        "NICHOLAS BOYNTON": "NICK BOYNTON",
        "NICHOLAS DRAZENOVIC": "NICK DRAZENOVIC",
        "NICKLAS BERGFORS": "NICLAS BERGFORS",
        "NICKLAS GROSSMAN": "NICKLAS GROSSMANN",
        "NICOLAS PETAN": "NIC PETAN",
        "NIKLAS KRONVALL": "NIKLAS KRONWALL",
        "NIKOLAI ANTROPOV": "NIK ANTROPOV",
        "NIKOLAI KULEMIN": "NIKOLAY KULEMIN",
        "NIKOLAI ZHERDEV": "NIKOLAY ZHERDEV",
        "OLIVIER MAGNAN-GRENIER": "OLIVIER MAGNAN",
        "PAT MAROON": "PATRICK MAROON",
        "PHILIP VARONE": "PHIL VARONE",
        "QUINTIN HUGHES": "QUINN HUGHES",
        "RAYMOND MACIAS": "RAY MACIAS",
        "RJ UMBERGER": "R.J. UMBERGER",
        "ROBERT BLAKE": "ROB BLAKE",
        "ROBERT EARL": "ROBBIE EARL",
        "ROBERT HOLIK": "BOBBY HOLIK",
        "ROBERT SCUDERI": "ROB SCUDERI",
        "RODNEY PELLEY": "ROD PELLEY",
        "SIARHEI KASTSITSYN": "SERGEI KOSTITSYN",
        "SIMEON VARLAMOV": "SEMYON VARLAMOV",
        "STAFFAN KRONVALL": "STAFFAN KRONWALL",
        "STEVEN REINPRECHT": "STEVE REINPRECHT",
        "TJ GALIARDI": "T.J. GALIARDI",
        "TJ HENSICK": "T.J. HENSICK",
        "TOBY ENSTROM": "TOBIAS ENSTROM",
        "TOMMY SESTITO": "TOM SESTITO",
        "VACLAV PROSPAL": "VINNY PROSPAL",
        "VINCENT HINOSTROZA": "VINNIE HINOSTROZA",
        "WILLIAM THOMAS": "BILL THOMAS",
        "ZACHARY ASTON-REESE": "ZACH ASTON-REESE",
        "ZACHARY SANFORD": "ZACH SANFORD",
        "ZACHERY STORTINI": "ZACK STORTINI",
        "MATTHEW MURRAY": "MATT MURRAY",
        "J-SEBASTIEN AUBIN": "JEAN-SEBASTIEN AUBIN",
        "JEFF DROUIN-DESLAURIERS": "JEFF DESLAURIERS",
        "NICHOLAS BAPTISTE": "NICK BAPTISTE",
        "OLAF KOLZIG": "OLIE KOLZIG",
        "STEPHEN VALIQUETTE": "STEVE VALIQUETTE",
        "THOMAS MCCOLLUM": "TOM MCCOLLUM",
        "TIMOTHY JR. THOMAS": "TIM THOMAS",
        "TIM GETTINGER": "TIMOTHY GETTINGER",
        "NICHOLAS SHORE": "NICK SHORE",
        "T.J. TYNAN": "TJ TYNAN",
        "ALEXIS LAFRENIÈRE": "ALEXIS LAFRENIERE",
        "ALEXIS LAFRENI?RE": "ALEXIS LAFRENIERE",
        "ALEXIS LAFRENIÃRE": "ALEXIS LAFRENIERE",
        'ALEXIS LAFRENIARE': 'ALEXIS LAFRENIERE',
        "TIM STÜTZLE": "TIM STUTZLE",
        "TIM ST?TZLE": "TIM STUTZLE",
        "TIM STÃTZLE": "TIM STUTZLE",
        "TIM STATZLE": "TIM STUTZLE",
        "JANI HAKANPÃ\x84Ã\x84": "JANI HAKANPAA",
        "EGOR SHARANGOVICH": "YEGOR SHARANGOVICH",
        "CALLAN FOOTE": "CAL FOOTE",
        "MATTIAS JANMARK-NYLEN": "MATTIAS JANMARK",
        "JOSH DUNNE": "JOSHUA DUNNE",
        "JANIS MOSER": "J.J. MOSER",
        "NICHOLAS PAUL": "NICK PAUL",
        "JACOB MIDDLETON": "JAKE MIDDLETON",
        "TOMMY NOVAK": "THOMAS NOVAK",
        "JOSHUA NORRIS": "JOSH NORRIS",
        "P.O JOSEPH": "PIERRE-OLIVIER JOSEPH",
        "MIKEY EYSSIMONT": "MICHAEL EYSSIMONT",
        "MATAJ  BLAMEL": "MATAJ BLAMEL",
        "MATEJ BLAMEL": "MATAJ BLAMEL",
        "VITTORIO MANCINI": "VICTOR MANCINI",
        "JOSHUA MAHURA": "JOSH MAHURA",
        "JOSEPH VELENO": "JOE VELENO",
        "ZACK BOLDUC": "ZACHARY BOLDUC",
        "JOSHUA BROWN": "JOSH BROWN",
        "JAKE LUCCHINI": "JACOB LUCCHINI",
        "EMIL LILLEBERG": "EMIL MARTINSEN LILLEBERG",
        "CAMERON ATKINSON": "CAM ATKINSON",
        "JURAJ SLAFKOVSKA": "JURAJ SLAFKOVSKY",
        "MARTIN FEHARVARY": "MARTIN FEHERVARY",
        "JOHN (JACK) ROSLOVIC": "JACK ROSLOVIC",
        "ANTHONY-JOHN (AJ) GREER": "A.J. GREER",
        "ALEX BARRÃ-BOULET": "ALEX BARRE-BOULET",
        "COLIN": "COLIN WHITE CAN",
        "CAMERON TALBOT":"CAM TALBOT",
        'DANIEL VLADAR': 'DAN VLADAR',
        'LUCAS GLENDENING': 'LUKE GLENDENING',
        'FREDDY GAUDREAU': 'FREDERICK GAUDREAU',
        'SAMUEL BLAIS': 'SAMMY BLAIS',
        'ISAC LUNDESTRAM': 'ISAC LUNDESTROM',
        'NATHAN LEGARE': 'NATHAN LAGARA',
        'NATHAN LEGARA': 'NATHAN LAGARA',
        'NATHAN LAGARE': 'NATHAN LAGARA',
        'SAMUEL MONTEMBAULT': 'SAM MONTENBAULT'
    }

    # Multiple name mappings (for .isin() checks)
    _NAME_CORRECTIONS_MULTI = {
        "BJ CROMBEEN": "B.J. CROMBEEN",
        "B.J CROMBEEN": "B.J. CROMBEEN",
        "BRANDON CROMBEEN": "B.J. CROMBEEN",
        "B J CROMBEEN": "B.J. CROMBEEN",
        "DAN CLEARY": "DANIEL CLEARY",
        "DANNY CLEARY": "DANIEL CLEARY",
        "MICHAËL BOURNIVAL": "MICHAEL BOURNIVAL",
        "MICHAÃ\x8bL BOURNIVAL": "MICHAEL BOURNIVAL",
        "J P DUMONT": "J-P DUMONT",
        "JEAN-PIERRE DUMONT": "J-P DUMONT",
        "P. J. AXELSSON": "P.J. AXELSSON",
        "PER JOHAN AXELSSON": "P.J. AXELSSON",
        "PK SUBBAN": "P.K. SUBBAN",
        "P.K SUBBAN": "P.K. SUBBAN",
        "PIERRE PARENTEAU": "P.A. PARENTEAU",
        "PIERRE-ALEX PARENTEAU": "P.A. PARENTEAU",
        "PIERRE-ALEXANDRE PARENTEAU": "P.A. PARENTEAU",
        "PA PARENTEAU": "P.A. PARENTEAU",
        "P.A PARENTEAU": "P.A. PARENTEAU",
        "P-A PARENTEAU": "P.A. PARENTEAU",
        "TJ OSHIE": "T.J. OSHIE",
        "T.J OSHIE": "T.J. OSHIE",
        "J.F. BERUBE": "J-F BERUBE",
        "JEAN-FRANCOIS BERUBE": "J-F BERUBE",
    }

    # Specific name corrections (matching ESPN function)
    ESPN_NAME_CORRECTIONS = {
        'PATRICK MAROON': 'PAT MAROON',
        'J T COMPHER': 'J.T. COMPHER',
        'J T MILLER': 'J.T. MILLER',
        'T J OSHIE': 'T.J. OSHIE',
        'ALEXIS LAFRENIERE': 'ALEXIS LAFRENIÈRE',
        'ALEXIS LAFRENI RE': 'ALEXIS LAFRENIÈRE',
        'TIM STUTZLE': 'TIM STÜTZLE',
        'TIM ST TZLE': 'TIM STÜTZLE',
        'T.J. BRODIE': 'TJ BRODIE',
        'MATTHEW IRWIN': 'MATT IRWIN',
        'STEVE KAMPFER': 'STEVEN KAMPFER',
        'JEFFREY TRUCHON-VIEL': 'JEFFREY VIEL',
        'ZACHARY JONES': 'ZAC JONES',
        'MITCH MARNER': 'MITCHELL MARNER',
        'MATHEW DUMBA': 'MATT DUMBA',
        'JOSHUA MORRISSEY': 'JOSH MORRISSEY',
        'P K SUBBAN': 'P.K. SUBBAN',
        'EGOR SHARANGOVICH': 'YEGOR SHARANGOVICH',
        'MAXIME COMTOIS': 'MAX COMTOIS',
        'NICHOLAS CAAMANO': 'NICK CAAMANO',
        'DANIEL CARCILLO': 'DAN CARCILLO',
        'ALEXANDER OVECHKIN': 'ALEX OVECHKIN',
        'MICHAEL CAMMALLERI': 'MIKE CAMMALLERI',
        'DAVE STECKEL': 'DAVID STECKEL',
        'JIM DOWD': 'JAMES DOWD',
        'MAXIME TALBOT': 'MAX TALBOT',
        'MIKE ZIGOMANIS': 'MICHAEL ZIGOMANIS',
        'VINNY PROSPAL': 'VACLAV PROSPAL',
        'MIKE YORK': 'MICHAEL YORK',
        'JACOB DOWELL': 'JAKE DOWELL',
        'MICHAEL RUPP': 'MIKE RUPP',
        'ALEXEI KOVALEV': 'ALEX KOVALEV',
        'SLAVA KOZLOV': 'VYACHESLAV KOZLOV',
        'JEFF HAMILTON': 'JEFFREY HAMILTON',
        'JOHNNY POHL': 'JOHN POHL',
        'DANIEL GIRARDI': 'DAN GIRARDI',
        'NIKOLAI ZHERDEV': 'NIKOLAY ZHERDEV',
        'J.P. DUMONT': 'J-P DUMONT',
        'DWAYNE KING': 'DJ KING',
        'JOHN ODUYA': 'JOHNNY ODUYA',
        'ROBERT SCUDERI': 'ROB SCUDERI',
        'DOUG MURRAY': 'DOUGLAS MURRAY',
        'VACLAV PROSPAL': 'VINNY PROSPAL',
        'RICH PEVERLY': 'RICH PEVERLEY',
        'JANIS MOSER': 'J.J. MOSER',
        'NICHOLAS PAUL': 'NICK PAUL',
        'JACOB MIDDLETON': 'JAKE MIDDLETON',
        'TOMMY NOVAK': 'THOMAS NOVAK',
        'JOHHNY BEECHER': 'JOHN BEECHER',
        'ALEXANDER BARKOV': 'ALEKSANDER BARKOV',
        'JOSHUA NORRIS': 'JOSH NORRIS',
        'P.O JOSEPH': 'PIERRE-OLIVIER JOSEPH',
        'MIKEY EYSSIMONT': 'MICHAEL EYSSIMONT',
        'MATAJ  BLAMEL': 'MATAJ BLAMEL',
        'VITTORIO MANCINI': 'VICTOR MANCINI',
        'JOSHUA MAHURA': 'JOSH MAHURA',
        'JOSEPH VELENO': 'JOE VELENO',
        'ZACK BOLDUC': 'ZACHARY BOLDUC',
        'JOSHUA BROWN': 'JOSH BROWN',
        'JAKE LUCCHINI': 'JACOB LUCCHINI',
        'EMIL LILLEBERG': 'EMIL MARTINSEN LILLEBERG',
        'CAMERON ATKINSON': 'CAM ATKINSON',
        'JURAJ SLAFKOVSKA': 'JURAJ SLAFKOVSKY',
        'MARTIN FEHARVARY': 'MARTIN FEHERVARY',
        'JOHN (JACK) ROSLOVIC': 'JACK ROSLOVIC',
        'ANTHONY-JOHN (AJ) GREER': 'A.J. GREER',
    }

    # Merge multi into main dict
    _NAME_CORRECTIONS.update(_NAME_CORRECTIONS_MULTI)

    _NAME_CORRECTIONS.update(ESPN_NAME_CORRECTIONS)

    name_corrections = _NAME_CORRECTIONS
    
    name = name_corrections.get(name, name)
    
    # Clean up multiple spaces
    name = re.sub(r' +', ' ', name)
    
    return name.strip()

def scrape_api_events(game_id, drop_description=True, shift_to_espn=False):
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
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with columns: coords_x, coords_y, event_player_1, event, 
        game_seconds, period, version (and optionally description)
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
        print(f'  ⏱️ API events network request: {net_duration:.2f}s')
        
        response.raise_for_status()
        
        # TIME: JSON parsing
        parse_start = time.time()
        api_data = json.loads(response.content)
        player_mapping_df = pd.DataFrame(api_data['rosterSpots'])
        player_mapping_df = player_mapping_df.assign(player = (player_mapping_df['firstName'].apply(lambda x: x['default']) + ' ' + player_mapping_df['lastName'].apply(lambda x: x['default'])).str.upper(),
                      link = 'https://assets.nhle.com/mugs/nhl/latest/' + player_mapping_df['playerId'].astype(str) + '.png',
                      id = player_mapping_df['playerId']).loc[:, ['player', 'link', 'id']]
        # Create dictionary mapping player ID to name for fast lookup
        player_mapping_dict = dict(zip(player_mapping_df['id'].astype(str), player_mapping_df['player']))
        parse_duration = time.time() - parse_start
        print(f'  ⏱️ API JSON parsing: {parse_duration:.2f}s')
    except Exception as e:
        raise KeyError(f"Failed to fetch NHL API data for game {game_id}: {e}")
    
    # Extract plays array
    if 'plays' not in api_data:
        raise KeyError(f"No 'plays' key found in NHL API response for game {game_id}")
    
    plays = api_data['plays']
    
    if not plays:
        # Return empty DataFrame with correct columns
        columns = ['coords_x', 'coords_y', 'event_player_1', 'event', 'game_seconds', 'period', 'version']
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
            })
    
    if not events_list:
        # Return empty DataFrame with correct columns
        columns = ['coords_x', 'coords_y', 'event_player_1', 'event', 'game_seconds', 'period', 'version']
        if not drop_description:
            columns.append('description')
        return pd.DataFrame(columns=columns)
    
    # Convert to DataFrame
    events_df = pd.DataFrame(events_list)
    
    # Filter out events without player names (matching ESPN behavior)
    # ESPN filters: events must have coords AND player names
    events_df = events_df[events_df['event_player_1'].notna()]
    
    # Normalize player names
    events_df['event_player_1'] = events_df['event_player_1'].apply(_normalize_player_name)
    
    # Filter again after normalization (in case normalization resulted in empty strings)
    events_df = events_df[events_df['event_player_1'] != '']
    
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
    final_columns = ['coords_x', 'coords_y', 'event_player_1', 'event', 'game_seconds', 'period', 'version']
    if not drop_description:
        final_columns.append('description')
    
    events_df = events_df[final_columns]
    
    return events_df

