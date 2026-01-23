import numpy as np
import pandas as pd
from bs4  import BeautifulSoup  # Keep for fallback/compatibility
from lxml import html, etree
import requests
import time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")
import sys
import json
from json import loads, dumps
import lxml
from requests import ConnectionError, ReadTimeout, ConnectTimeout, HTTPError, Timeout
import xml
import re
from natsort import natsorted
import xml.etree.ElementTree as ET
import xmltodict
from xml.parsers.expat import ExpatError
from requests.exceptions import ChunkedEncodingError
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from TopDownHockey_Scraper.scrape_nhl_api_events import scrape_api_events

print('Successfully did local install plus update - OPTIMIZED VERSION (Round 1: _append(), Round 2: name corrections, Round 3: vectorization, Round 4: parallel network requests)')

# ========== OPTIMIZATIONS ==========
# Create a persistent session with connection pooling
_session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=10,
    pool_maxsize=20,
    max_retries=2
)
_session.mount('http://', _adapter)
_session.mount('https://', _adapter)

# Compile regex patterns once for reuse
_BBORDER_PATTERN = re.compile('.*bborder.*')
_ZONE_PATTERN = re.compile(r'(\S+?) Zone')
_PLAYER_NUM_PATTERN = re.compile(r'[#-]\s*(\d+)')
_MATCH_GAME_PATTERN = re.compile(r'Match|Game')
_PARENTHESIS_PATTERN = re.compile(r'\((.*?)\)')
_MULTI_SPACE_PATTERN = re.compile(r' +')
_CAPTAIN_A_PATTERN = re.compile(r' \(A\)$')
_CAPTAIN_C_PATTERN = re.compile(r' \(C\)$')

# ========== PARALLEL FETCHING HELPERS ==========
def _fetch_url(url, max_retries=3, base_delay=2, **kwargs):
    """
    Helper function to fetch URL with session for use in ThreadPoolExecutor.
    Includes retry logic with exponential backoff for transient failures.
    """
    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            response = _session.get(url, **kwargs)
            response.raise_for_status()
            return response
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)  # Exponential backoff: 2, 4, 8 seconds
                print(f"  Fetch failed for {url.split('/')[-1]} (attempt {attempt + 1}/{max_retries + 1}): {e}")
                print(f"  Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"  All {max_retries + 1} attempts failed for {url.split('/')[-1]}")
                raise last_exception
# ===============================================

team_names = ['ANAHEIM DUCKS',
 'ARIZONA COYOTES',
 'ATLANTA THRASHERS',
 'BOSTON BRUINS',
 'BUFFALO SABRES',
 'CALGARY FLAMES',
 'CHICAGO BLACKHAWKS',
 'COLORADO AVALANCHE',
 'COLUMBUS BLUE JACKETS',
 'DALLAS STARS',
 'DETROIT RED WINGS',
 'EDMONTON OILERS',
 'FLORIDA PANTHERS',
 'LOS ANGELES KINGS',
 'MINNESOTA WILD',
 'MONTRÉAL CANADIENS',
 'MONTREAL CANADIENS',
 'NASHVILLE PREDATORS',
 'NEW JERSEY DEVILS',
 'NEW YORK ISLANDERS',
 'NEW YORK RANGERS',
 'OTTAWA SENATORS',
 'PHILADELPHIA FLYERS',
 'PITTSBURGH PENGUINS',
 'PHOENIX COYOTES',
 'CAROLINA HURRICANES',
 'SAN JOSE SHARKS',
 'ST. LOUIS BLUES',
 'TAMPA BAY LIGHTNING',
 'TORONTO MAPLE LEAFS',
 'UTAH MAMMOTH',
 'VANCOUVER CANUCKS',
 'VEGAS GOLDEN KNIGHTS',
 'WASHINGTON CAPITALS',
 'WINNIPEG JETS',
 'SEATTLE KRAKEN']

# ewc stands for "Events we care about."

ewc = ['SHOT', 'HIT', 'BLOCK', 'MISS', 'GIVE', 'TAKE', 'GOAL']

# ========== OPTIMIZATION: Name Correction Dictionaries ==========
# Convert nested np.where() chains to fast dictionary lookups
# This provides 50-90% speedup on name correction operations

# ==================================

from TopDownHockey_Scraper.name_corrections import NAME_CORRECTIONS, normalize_player_name

# Lazy-loaded portrait links for ID fallback in fix_missing()
_portrait_links_dict = None

def _get_portrait_links_dict():
    """Load portrait_links.csv as name→id dict (lazy loaded, cached)"""
    global _portrait_links_dict
    if _portrait_links_dict is None:
        try:
            from importlib.resources import files
            data_path = files('TopDownHockey_Scraper').joinpath('portrait_links.csv')
            with data_path.open('r') as f:
                df = pd.read_csv(f)
        except (ImportError, TypeError):
            import pkg_resources
            data_path = pkg_resources.resource_filename('TopDownHockey_Scraper', 'portrait_links.csv')
            df = pd.read_csv(data_path)

        # Create name→id mapping (uppercase names for matching)
        _portrait_links_dict = dict(zip(df['player'].str.upper(), df['id']))
    return _portrait_links_dict

def subtract_from_twenty_minutes(time_string):
    # Parse the input time string
    minutes, seconds = map(int, time_string.split(':'))
    
    # Convert to total seconds
    input_seconds = minutes * 60 + seconds
    twenty_minutes_seconds = 20 * 60  # 1200 seconds
    
    # Calculate the difference
    difference_seconds = twenty_minutes_seconds - input_seconds
    
    # Convert back to MM:SS format
    result_minutes = difference_seconds // 60
    result_seconds = difference_seconds % 60
    
    # Format the result
    return f"{result_minutes}:{result_seconds:02d}"

def convert_clock_to_seconds(clock):
    min = int(clock.split(':')[0])
    sec = int(clock.split(':')[1])
    seconds = min * 60 + sec
    return seconds

def convert_seconds_to_clock(seconds):

    if len(str(int(seconds/60))) == 1:
        minutes_string = '0' + str(int(seconds/60))
    else:
        minutes_string = str(int(seconds/60))

    remainder = seconds - (60 * int(seconds/60))

    if len(str(remainder)) == 1:
        remainder_string = '0' + str(remainder)
    else:
        remainder_string = str(remainder)

    return minutes_string + ':' + remainder_string

def scrape_schedule(start_date, end_date):
    
    """
    Scrape the NHL's API and get a schedule back.
    """
    
    url = 'https://statsapi.web.nhl.com/api/v1/schedule?startDate=' + start_date + '&endDate=' + end_date
    page = _session.get(url, timeout=30)
    loaddict = json.loads(page.content)
    date_list = (loaddict['dates'])
    date_df = pd.DataFrame(date_list)
    
    # OPTIMIZED: Use list + concat instead of repeated _append()
    gamedf_list = []
    for i in range (0, len(date_df)):
        datedf = pd.DataFrame(date_df.games.iloc[i])
        gamedf_list.append(datedf)
    gamedf = pd.concat(gamedf_list, ignore_index=True) if gamedf_list else pd.DataFrame()
    global team_df
    team_df = pd.DataFrame(gamedf['teams'].values.tolist(), index = gamedf.index)
    away_df = pd.DataFrame(team_df['away'].values.tolist(), index = team_df.index)
    home_df = pd.DataFrame(team_df['home'].values.tolist(), index = team_df.index)
    away_team_df = pd.DataFrame(away_df['team'].values.tolist(), index = away_df.index)
    home_team_df = pd.DataFrame(home_df['team'].values.tolist(), index = home_df.index)

    gamedf = gamedf.assign(
        state = pd.DataFrame(gamedf['status'].values.tolist(), index = gamedf.index)['detailedState'],
        homename = home_team_df['name'],
        homeid = home_team_df['id'],
        homescore = home_df['score'],
        awayname = away_team_df['name'],
        awayid = away_team_df['id'],
        awayscore = away_df['score'],
        venue = pd.DataFrame(gamedf['venue'].values.tolist(), index = gamedf.index)['name'],
        gameDate = pd.to_datetime(gamedf['gameDate']).dt.tz_convert('EST')
    )

    gamedf = gamedf.loc[:, ['gamePk', 'link', 'gameType', 'season', 'gameDate','homeid', 'homename',  'homescore','awayid', 'awayname',  'awayscore', 'state', 'venue']].rename(
        columns = {'gamePk':'ID', 'gameType':'type', 'gameDate':'date'})
    
    gamedf['type']

    return(gamedf)

def hs_strip_html(td):
    """
    Function from Harry Shomer's Github
    
    Strip html tags and such 
    
    :param td: pbp (list of lxml elements)
    
    :return: list of plays (which contain a list of info) stripped of html
    """
    for y in range(len(td)):
        # Get the 'br' tag for the time column...this get's us time remaining instead of elapsed and remaining combined
        if y == 3:
            td[y] = td[y].text_content()   # This gets us elapsed and remaining combined-< 3:0017:00
            index = td[y].find(':')
            td[y] = td[y][:index+3]
        elif (y == 6 or y == 7) and td[0] != '#':
            # 6 & 7-> These are the player 1 ice one's
            # The second statement controls for when it's just a header
            baz = td[y].xpath('.//td')
            bar = [baz[z] for z in range(len(baz)) if z % 4 != 0]  # Because of previous step we get repeats...delete some

            # The setup in the list is now: Name/Number->Position->Blank...and repeat
            # Now strip all the html
            players = []
            for i in range(len(bar)):
                if i % 3 == 0:
                    try:
                        font_elem = bar[i].xpath('.//font')
                        if font_elem:
                            name = return_name_html(font_elem[0].get('title', ''))
                        else:
                            name = ''
                        number = bar[i].text_content().strip('\n')  # Get number and strip leading/trailing newlines
                    except (KeyError, IndexError):
                        name = ''
                        number = ''
                elif i % 3 == 1:
                    if name != '':
                        position = bar[i].text_content()
                        players.append([name, number, position])

            td[y] = players
        else:
            td[y] = td[y].text_content()

    return td

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

def group_if_not_none(result):
    if result is not None:
        result = result.group()
    return(result)

def scrape_html_roster(season, game_id, page=None, verbose=False):
    """
    Scrape HTML roster page.
    
    Args:
        season: Season string (e.g., '20242025')
        game_id: Game ID string (e.g., '020333')
        page: Optional pre-fetched requests.Response object. If None, will fetch the page.
        verbose: If True, print detailed timing information
    
    Returns:
        DataFrame with roster information
    """
    if page is None:
        url = 'http://www.nhl.com/scores/htmlreports/' + season + '/RO0' + game_id + '.HTM'
        
        # TIME: Roster network request
        net_start = time.time()
        page = _session.get(url, timeout=10)
        net_duration = time.time() - net_start
        if verbose:
            try:
                print(f'  ⏱️ Roster network request: {net_duration:.2f}s')
            except Exception:
                pass
    
    # OPTIMIZED: Use lxml directly instead of BeautifulSoup for faster parsing
    if type(page) == str:
        doc = html.fromstring(page)
    else:
        doc = html.fromstring(page.content.decode('ISO-8859-1'))

    # XPath to find td elements with align='center', class containing 'teamHeading' and 'border', width='50%'
    teamsoup = doc.xpath("//td[@align='center' and @width='50%' and contains(@class, 'teamHeading') and contains(@class, 'border')]")
    away_team = teamsoup[0].text_content() if len(teamsoup) > 0 else ''
    home_team = teamsoup[1].text_content() if len(teamsoup) > 1 else ''

    # XPath to find tables with specific attributes, then get td elements from the 3rd table (index 2)
    tables = doc.xpath("//table[@align='center' and @border='0' and @cellpadding='0' and @cellspacing='0' and @width='100%']")
    home_player_soup = tables[2].xpath(".//td") if len(tables) > 2 else []
    # Convert lxml elements to text content
    home_player_soup = [elem.text_content() if hasattr(elem, 'text_content') else str(elem) for elem in home_player_soup]

    length = int(len(home_player_soup)/3)

    home_player_df = pd.DataFrame(np.array(home_player_soup).reshape(length, 3))

    home_player_df.columns = home_player_df.iloc[0]

    home_player_df = home_player_df.drop(0).assign(team = 'home', team_name = home_team)

    # Get away player data from 2nd table (index 1)
    away_player_soup = tables[1].xpath(".//td") if len(tables) > 1 else []
    # Convert lxml elements to text content
    away_player_soup = [elem.text_content() if hasattr(elem, 'text_content') else str(elem) for elem in away_player_soup]

    length = int(len(away_player_soup)/3)

    away_player_df = pd.DataFrame(np.array(away_player_soup).reshape(length, 3))

    away_player_df.columns = away_player_df.iloc[0]

    away_player_df = away_player_df.drop(0).assign(team = 'away', team_name = away_team)

    #global home_scratch_soup

    if len(tables) > 3:

        try:

            home_scratch_soup = tables[4].xpath(".//td") if len(tables) > 4 else []
            # Convert lxml elements to text content
            home_scratch_soup = [elem.text_content() if hasattr(elem, 'text_content') else str(elem) for elem in home_scratch_soup]

            if len(home_scratch_soup)>1:

                length = int(len(home_scratch_soup)/3)

                home_scratch_df = pd.DataFrame(np.array(home_scratch_soup).reshape(length, 3))

                home_scratch_df.columns = home_scratch_df.iloc[0]

                home_scratch_df = home_scratch_df.drop(0).assign(team = 'home', team_name = home_team)

        except Exception as e:
            print(e)
            print('No home scratch soup')
            home_scratch_df = pd.DataFrame()

    if 'home_scratch_df' not in locals():
        
        home_scratch_df = pd.DataFrame()
        
    if len(tables) > 2:

        try:

            away_scratch_soup = tables[3].xpath(".//td") if len(tables) > 3 else []
            # Convert lxml elements to text content
            away_scratch_soup = [elem.text_content() if hasattr(elem, 'text_content') else str(elem) for elem in away_scratch_soup]
            
            if len(away_scratch_soup)>1:

                length = int(len(away_scratch_soup)/3)

                away_scratch_df = pd.DataFrame(np.array(away_scratch_soup).reshape(length, 3))

                away_scratch_df.columns = away_scratch_df.iloc[0]

                away_scratch_df = away_scratch_df.drop(0).assign(team = 'away', team_name = away_team)

        except Exception as e:
            print(e)
            print('No away scratch soup')
            away_scratch_df = pd.DataFrame()
        
    if 'away_scratch_df' not in locals():
        
        away_scratch_df = pd.DataFrame()

    player_df = pd.concat([home_player_df, away_player_df]).assign(status = 'player')
    scratch_df = pd.concat([home_scratch_df, away_scratch_df]).assign(status = 'scratch')
    roster_df = pd.concat([player_df, scratch_df])

    roster_df = roster_df.assign(team = np.where(roster_df.team=='CANADIENS MONTREAL', 'MONTREAL CANADIENS', roster_df.team))

    roster_df = roster_df.assign(team = np.where(roster_df.team=='MONTRÉAL CANADIENS', 'MONTREAL CANADIENS', roster_df.team))

    # FIX NAMES

    roster_df = roster_df.rename(columns = {'Nom/Name':'Name'})

    roster_df.Name = roster_df.Name.apply(lambda x: _CAPTAIN_A_PATTERN.sub('', x).strip())
    roster_df.Name = roster_df.Name.apply(lambda x: _CAPTAIN_C_PATTERN.sub('', x).strip())

    # OPTIMIZED: Batch string replacements instead of conditional np.where()
    # Max Pacioretty doesn't exist in ESPN in 2009-2010, sadly.
    roster_df['Name'] = (roster_df['Name']
        .str.replace('ALEXANDRE ', 'ALEX ', regex=False)
        .str.replace('ALEXANDER ', 'ALEX ', regex=False)
        .str.replace('CHRISTOPHER ', 'CHRIS ', regex=False))

    # OPTIMIZED: Use dictionary lookup instead of nested np.where() chains
    # This provides 50-90% speedup on name corrections

    # OPTIMIZED: Already handled by dictionary lookup above
    # (These names are already in _NAME_CORRECTIONS)

    roster_df['Name'] = np.where((roster_df['Name']=="SEBASTIAN AHO") & (roster_df['Pos']=='D'), 'SEBASTIAN AHO SWE', roster_df['Name'])
    roster_df['Name'] = np.where((roster_df['Name']=="ELIAS PETTERSSON") & (roster_df['Pos']=='D'), 'ELIAS PETTERSSON(D)', roster_df['Name'])
    roster_df['Name'] = np.where((roster_df['Name']=="COLIN WHITE") & (roster_df['Pos']=='D'), 'COLIN WHITE CAN', roster_df['Name'])
    roster_df['Name'] = np.where((roster_df['Name']=="SEAN COLLINS") & (roster_df['Pos']=='D'), 'SEAN COLLINS CAN', roster_df['Name'])
    roster_df['Name'] = np.where((roster_df['Name']=="ALEX PICARD") & (roster_df['Pos']!='D'), 'ALEX PICARD F', roster_df['Name'])
    roster_df['Name'] = np.where((roster_df['Name']=="ERIK GUSTAFSSON") & (int(season)<20132014), 'ERIK GUSTAFSSON 88', roster_df['Name'])
    roster_df['Name'] = np.where((roster_df['Name']=="MIKKO LEHTONEN") & (int(season)<20202021), 'MIKKO LEHTONEN F', roster_df['Name'])
    roster_df['Name'] = np.where(roster_df['Name']=='ALEX BARRÃ-BOULET', 'ALEX BARRE-BOULET', roster_df['Name'])
    roster_df['Name'] = np.where(roster_df['Name']=='COLIN', 'COLIN WHITE CAN', roster_df['Name'])

    # OPTIMIZED: Already handled by dictionary lookup above
    # (These names are already in _NAME_CORRECTIONS)

    roster_df['Name'] = roster_df['Name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()

    roster_df['Name'] = np.where(roster_df['Name']== "JURAJ SLAFKOVSKA" , "JURAJ SLAFKOVSKY", roster_df['Name']) # Need to do this after normalization, only then he becomes Slafkovska?
    roster_df['Name'] = np.where(roster_df['Name']== "JOHN (JACK) ROSLOVIC" , "JACK ROSLOVIC", roster_df['Name'])
    roster_df['Name'] = np.where(roster_df['Name']== "ANTHONY-JOHN (AJ) GREER" , "A.J. GREER", roster_df['Name'])

    roster_df['Name'] = np.where(roster_df['Name']== "MARTIN FEHARVARY" , "MARTIN FEHERVARY", roster_df['Name']) 

    roster_df['Name'] = np.where(roster_df['Name']== "MATAJ  BLAMEL" , "MATAJ BLAMEL", roster_df['Name']) 

    roster_df['Name'] = roster_df['Name'].str.replace('  ', ' ')

    roster_df['Name'] = roster_df['Name'].apply(lambda x: normalize_player_name(x))

    return roster_df 

def scrape_html_shifts(season, game_id, live = True, home_page=None, away_page=None, summary = None, roster_cache = None, verbose=False):
    """
    Scrape HTML shifts pages.
    
    Args:
        season: Season string (e.g., '20242025')
        game_id: Game ID string (e.g., '020333')
        live: Boolean flag for live games
        home_page: Optional pre-fetched requests.Response object for home shifts page. If None, will fetch.
        away_page: Optional pre-fetched requests.Response object for away shifts page. If None, will fetch.
        summary: Optional summary page for goalie data
        roster_cache: Roster cache for goalie names
        verbose: If True, print detailed timing information
    
    Returns:
        DataFrame with shift information
    """
    goalie_names = roster_cache[roster_cache.Pos=='G'].Name.unique().tolist()
    home_goalie_names = roster_cache[(roster_cache.Pos=='G') & (roster_cache.team=='home')].Name.unique().tolist()
    away_goalie_names = roster_cache[(roster_cache.Pos=='G') & (roster_cache.team=='away')].Name.unique().tolist()

    if home_page is None:
        url = 'http://www.nhl.com/scores/htmlreports/' + season + '/TH0' + game_id + '.HTM'
        
        # TIME: Home shifts network request
        net_start = time.time()
        home_page = _session.get(url, timeout=10)
        net_duration = time.time() - net_start
        if verbose:
            try:
                print(f'  ⏱️ Home shifts network request: {net_duration:.2f}s')
            except Exception:
                pass

    # NOTE: Keeping BeautifulSoup for shifts parsing for now due to complex class matching
    # lxml optimization applied to events parsing (major speedup achieved there)
    if type(home_page) == str:
        home_soup = BeautifulSoup(home_page)
    else:
        home_soup = BeautifulSoup(home_page.content, 'lxml')
    found = home_soup.find_all('td', {'class':['playerHeading + border', 'lborder + bborder']})
    if len(found)==0:
        raise IndexError('This game has no shift data.')
    thisteam = home_soup.find('td', {'align':'center', 'class':'teamHeading + border'}).get_text()
    # Normalize Montreal team name (handles encoding issues)
    if 'MONTR' in thisteam and 'CANAD' in thisteam:
        thisteam = 'MONTREAL CANADIENS'

    players = dict()

    # OPTIMIZED: Reduce repeated string operations
    for i in range(len(found)):
        line = found[i].get_text()
        if line == '25 PETTERSSON, ELIAS':
            line = '25 PETTERSSON(D), ELIAS'
        if ', ' in line:
            # OPTIMIZED: Split once and reuse
            name_parts = line.split(',')
            if len(name_parts) >= 2:
                number_last = name_parts[0].split(' ', 1)  # Split only once
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
            players[full_name]['shifts'].append(line)  # Use append instead of extend([line])

    # OPTIMIZED: Use list + concat instead of repeated _append()
    alldf_list = []
    for key in players.keys(): 
        shifts_array = np.array(players[key]['shifts'])
        length = (len(shifts_array) // 5) * 5
        shifts_array = shifts_array[:length]
        df = pd.DataFrame(shifts_array.reshape(-1, 5)).rename(
        columns = {0:'shift_number', 1:'period', 2:'shift_start', 3:'shift_end', 4:'duration'})
        df = df.assign(name = players[key]['name'],
                        number = players[key]['number'],
                        team = thisteam,
                        venue = "home")
        alldf_list.append(df)

    home_shifts = pd.concat(alldf_list, ignore_index=True) if alldf_list else pd.DataFrame()

    if live == True:

        home_shifts = home_shifts.assign(shift_number = home_shifts.shift_number.astype(int))
        home_shifts = home_shifts.assign(number = home_shifts.number.astype(int))

        found = home_soup.find_all('td', {'class':['playerHeading + border', 'bborder + lborder +']})
        if len(found)==0:
            raise IndexError('This game has no shift data.')
        thisteam = home_soup.find('td', {'align':'center', 'class':'teamHeading + border'}).get_text()
        # Normalize Montreal team name (handles encoding issues)
        if 'MONTR' in thisteam and 'CANAD' in thisteam:
            thisteam = 'MONTREAL CANADIENS'

        players = dict()

        for i in range(len(found)):
            line = found[i].get_text()
            if line == '25 PETTERSSON, ELIAS':
                line = '25 PETTERSSON(D), ELIAS'
            if ', ' in line:
                # OPTIMIZED: Split once and reuse
                name_parts = line.split(',')
                if len(name_parts) >= 2:
                    number_last = name_parts[0].split(' ', 1)  # Split only once
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
                players[full_name]['shifts'].append(line)  # Use append instead of extend([line])

        # OPTIMIZED: Use list + concat instead of repeated _append()
        alldf_list = []
        for key in players.keys(): 
            length = length = int(len(players[key]['shifts'])/6)
            df = df = pd.DataFrame(np.array((players[key]['shifts'])).reshape(length, 6)).rename(
            columns = {0:'period', 1:'shifts', 2:'avg', 3:'TOI', 4:'EV Total', 5:'PP Total'})
            df = df.assign(name = players[key]['name'],
                            number = players[key]['number'],
                            team = thisteam,
                            venue = "home")
            alldf_list.append(df)
            
        home_extra_shifts = pd.concat(alldf_list, ignore_index=True) if alldf_list else pd.DataFrame()

        # Trigger: There is no home goalie for this period and we're not about to pull one from the extra shifts. 

        if len(home_shifts[(home_shifts.period==max(home_shifts.period)) & (home_shifts.name.isin(home_goalie_names))]) == 0 and len(home_extra_shifts[home_extra_shifts.name.isin(home_goalie_names)]) == 0:

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

            goalie_summary = goalie_summary[((goalie_summary.team==thisteam) | (('CANADIENS' in thisteam) & (goalie_summary.team.str.contains('CANADIENS')))) & ~(pd.isna(goalie_summary['TOI']))]

            goalie_summary = goalie_summary.assign(name = 
                goalie_summary.name.str.split(', ').str[-1] + ' ' + goalie_summary.name.str.split(', ').str[0]
            )

            goalie_summary.name = goalie_summary.name.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()

            goalie_summary = goalie_summary.assign(period = max(home_shifts.period), shifts = '1', avg = goalie_summary.TOI, venue = 'home').loc[:, home_extra_shifts.columns]

            home_extra_shifts = pd.concat([home_extra_shifts, goalie_summary])

        home_extra_shifts = home_extra_shifts.assign(TOI_seconds_summary = home_extra_shifts.TOI.apply(lambda x: convert_clock_to_seconds(x)))

        home_extra_shifts = home_extra_shifts.merge(
            home_shifts.assign(toi_secs = home_shifts.duration.apply(lambda x: convert_clock_to_seconds(x))
            ).groupby(['name', 'period'])['toi_secs'].sum().reset_index(),
            how = 'left'
        ).fillna(0)

        home_extra_shifts['toi_secs'] = home_extra_shifts['toi_secs'].astype(int)

        home_extra_shifts = home_extra_shifts.assign(toi_diff = abs(home_extra_shifts.toi_secs - home_extra_shifts.TOI_seconds_summary))

        shifts_needing_to_be_added = home_extra_shifts[home_extra_shifts.toi_diff!=0]

        if len(shifts_needing_to_be_added) > 0:

            latest_shift_end = home_shifts.assign(period_secs = home_shifts.shift_end.str.split(' / ').str[0].apply(lambda x: convert_clock_to_seconds(x)))[
            home_shifts.period==max(home_shifts.period)
            ].sort_values(by = 'period_secs', ascending = False).period_secs.iloc[0]

            max_toi = shifts_needing_to_be_added.TOI.apply(lambda x: convert_clock_to_seconds(x)).max()

            overage = max_toi - latest_shift_end

            if overage > 0:
                shifts_needing_to_be_added.toi_diff = shifts_needing_to_be_added.toi_diff - overage

            home_clock_time_now = convert_seconds_to_clock(latest_shift_end)

            home_clock_period = max(home_shifts.period.astype(int))

            start_times_seconds = home_clock_time_now

            import math

            shifts_needing_to_be_added = shifts_needing_to_be_added.assign(
                shift_start = ((convert_clock_to_seconds(home_clock_time_now) - shifts_needing_to_be_added.toi_diff).apply(lambda x: convert_seconds_to_clock(x)).astype(str)
                + ' / ' + (convert_clock_to_seconds(home_clock_time_now) - shifts_needing_to_be_added.toi_diff).apply(lambda x: convert_seconds_to_clock(x)).astype(str).apply(lambda x: subtract_from_twenty_minutes(x))),
                shift_end = home_clock_time_now + ' / ' + subtract_from_twenty_minutes(home_clock_time_now),
                duration = shifts_needing_to_be_added.toi_diff
                                )

            shifts_needing_to_be_added = shifts_needing_to_be_added.assign(
                duration = shifts_needing_to_be_added.toi_diff.apply(lambda x: convert_seconds_to_clock(x))
            )

            shifts_needing_to_be_added = shifts_needing_to_be_added.merge(
                home_shifts.assign(shift_number = home_shifts.shift_number.astype(int)).groupby('name')['shift_number'].max().reset_index().rename(columns = {'shift_number':'prior_max_shift'}),
                how = 'left'
            ).fillna(0)

            shifts_needing_to_be_added = shifts_needing_to_be_added.assign(shift_number = shifts_needing_to_be_added.prior_max_shift + 1)

            shifts_needing_to_be_added.shift_number = shifts_needing_to_be_added.shift_number.astype(int)

            shifts_needing_to_be_added = shifts_needing_to_be_added.loc[:, ['shift_number', 'period', 'shift_start', 'shift_end', 'duration', 'name', 'number', 'team', 'venue']]

            shifts_needing_to_be_added['number'] = shifts_needing_to_be_added['number'].astype(int)

            home_shifts = pd.concat([home_shifts, shifts_needing_to_be_added]).sort_values(by = ['number', 'period', 'shift_number'])

        elif len(shifts_needing_to_be_added) == 0:
            home_clock_period = None
            home_clock_time_now = None

    if away_page is None:
        url = 'http://www.nhl.com/scores/htmlreports/' + season + '/TH0' + game_id + '.HTM'
        
        # TIME: away shifts network request
        net_start = time.time()
        away_page = _session.get(url, timeout=10)
        net_duration = time.time() - net_start
        if verbose:
            try:
                print(f'  ⏱️ away shifts network request: {net_duration:.2f}s')
            except Exception:
                pass

    # NOTE: Keeping BeautifulSoup for shifts parsing for now due to complex class matching
    # lxml optimization applied to events parsing (major speedup achieved there)
    if type(away_page) == str:
        away_soup = BeautifulSoup(away_page)
    else:
        away_soup = BeautifulSoup(away_page.content, 'lxml')
    found = away_soup.find_all('td', {'class':['playerHeading + border', 'lborder + bborder']})
    if len(found)==0:
        raise IndexError('This game has no shift data.')
    thisteam = away_soup.find('td', {'align':'center', 'class':'teamHeading + border'}).get_text()

    players = dict()

    # OPTIMIZED: Reduce repeated string operations
    for i in range(len(found)):
        line = found[i].get_text()
        if line == '25 PETTERSSON, ELIAS':
            line = '25 PETTERSSON(D), ELIAS'
        if ', ' in line:
            # OPTIMIZED: Split once and reuse
            name_parts = line.split(',')
            if len(name_parts) >= 2:
                number_last = name_parts[0].split(' ', 1)  # Split only once
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
            players[full_name]['shifts'].append(line)  # Use append instead of extend([line])

    # OPTIMIZED: Use list + concat instead of repeated _append()
    alldf_list = []
    for key in players.keys(): 
        shifts_array = np.array(players[key]['shifts'])
        length = (len(shifts_array) // 5) * 5
        shifts_array = shifts_array[:length]
        df = pd.DataFrame(shifts_array.reshape(-1, 5)).rename(
        columns = {0:'shift_number', 1:'period', 2:'shift_start', 3:'shift_end', 4:'duration'})
        df = df.assign(name = players[key]['name'],
                        number = players[key]['number'],
                        team = thisteam,
                        venue = "away")
        alldf_list.append(df)

    away_shifts = pd.concat(alldf_list, ignore_index=True) if alldf_list else pd.DataFrame()

    if live == True:

        away_shifts = away_shifts.assign(shift_number = away_shifts.shift_number.astype(int))
        away_shifts = away_shifts.assign(number = away_shifts.number.astype(int))

        found = away_soup.find_all('td', {'class':['playerHeading + border', 'bborder + lborder +']})
        if len(found)==0:
            raise IndexError('This game has no shift data.')
        thisteam = away_soup.find('td', {'align':'center', 'class':'teamHeading + border'}).get_text()

        players = dict()

        for i in range(len(found)):
            line = found[i].get_text()
            if line == '25 PETTERSSON, ELIAS':
                line = '25 PETTERSSON(D), ELIAS'
            if ', ' in line:
                # OPTIMIZED: Split once and reuse
                name_parts = line.split(',')
                if len(name_parts) >= 2:
                    number_last = name_parts[0].split(' ', 1)  # Split only once
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
                players[full_name]['shifts'].append(line)  # Use append instead of extend([line])

        # OPTIMIZED: Use list + concat instead of repeated _append()
        alldf_list = []
        for key in players.keys(): 
            length = length = int(len(players[key]['shifts'])/6)
            df = df = pd.DataFrame(np.array((players[key]['shifts'])).reshape(length, 6)).rename(
            columns = {0:'period', 1:'shifts', 2:'avg', 3:'TOI', 4:'EV Total', 5:'PP Total'})
            df = df.assign(name = players[key]['name'],
                            number = players[key]['number'],
                            team = thisteam,
                            venue = "away")
            alldf_list.append(df)
            
        away_extra_shifts = pd.concat(alldf_list, ignore_index=True) if alldf_list else pd.DataFrame()

        # Trigger: There is no away goalie for this period and we're not about to pull one from the extra shifts. 

        if len(away_shifts[(away_shifts.period==max(away_shifts.period)) & (away_shifts.name.isin(away_goalie_names))]) == 0 and len(away_extra_shifts[away_extra_shifts.name.isin(away_goalie_names)]) == 0:

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

            goalie_summary = goalie_summary[((goalie_summary.team==thisteam) | (('CANADIENS' in thisteam) & (goalie_summary.team.str.contains('CANADIENS')))) & ~(pd.isna(goalie_summary['TOI']))]

            goalie_summary = goalie_summary.assign(name = 
                goalie_summary.name.str.split(', ').str[-1] + ' ' + goalie_summary.name.str.split(', ').str[0]
            )

            goalie_summary.name = goalie_summary.name.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()

            goalie_summary = goalie_summary.assign(period = max(away_shifts.period), shifts = '1', avg = goalie_summary.TOI, venue = 'away').loc[:, away_extra_shifts.columns]

            away_extra_shifts = pd.concat([away_extra_shifts, goalie_summary])

        away_extra_shifts = away_extra_shifts.assign(TOI_seconds_summary = away_extra_shifts.TOI.apply(lambda x: convert_clock_to_seconds(x)))

        away_extra_shifts = away_extra_shifts.merge(
            away_shifts.assign(toi_secs = away_shifts.duration.apply(lambda x: convert_clock_to_seconds(x))
            ).groupby(['name', 'period'])['toi_secs'].sum().reset_index(),
            how = 'left'
        ).fillna(0)

        away_extra_shifts['toi_secs'] = away_extra_shifts['toi_secs'].astype(int)

        away_extra_shifts = away_extra_shifts.assign(toi_diff = abs(away_extra_shifts.toi_secs - away_extra_shifts.TOI_seconds_summary))

        shifts_needing_to_be_added = away_extra_shifts[away_extra_shifts.toi_diff!=0]

        if len(shifts_needing_to_be_added) > 0:

            latest_shift_end = away_shifts.assign(period_secs = away_shifts.shift_end.str.split(' / ').str[0].apply(lambda x: convert_clock_to_seconds(x)))[
            away_shifts.period==max(away_shifts.period)
            ].sort_values(by = 'period_secs', ascending = False).period_secs.iloc[0]

            max_toi = shifts_needing_to_be_added.TOI.apply(lambda x: convert_clock_to_seconds(x)).max()

            overage = max_toi - latest_shift_end

            if overage > 0:
                shifts_needing_to_be_added.toi_diff = shifts_needing_to_be_added.toi_diff - overage

            away_clock_time_now = convert_seconds_to_clock(latest_shift_end)

            away_clock_period = max(away_shifts.period.astype(int))

            start_times_seconds = away_clock_time_now

            import math

            shifts_needing_to_be_added = shifts_needing_to_be_added.assign(
                shift_start = ((convert_clock_to_seconds(away_clock_time_now) - shifts_needing_to_be_added.toi_diff).apply(lambda x: convert_seconds_to_clock(x)).astype(str)
                + ' / ' + (convert_clock_to_seconds(away_clock_time_now) - shifts_needing_to_be_added.toi_diff).apply(lambda x: convert_seconds_to_clock(x)).astype(str).apply(lambda x: subtract_from_twenty_minutes(x))),
                shift_end = away_clock_time_now + ' / ' + subtract_from_twenty_minutes(away_clock_time_now),
                duration = shifts_needing_to_be_added.toi_diff
                                )

            shifts_needing_to_be_added = shifts_needing_to_be_added.assign(
                duration = shifts_needing_to_be_added.toi_diff.apply(lambda x: convert_seconds_to_clock(x))
            )

            shifts_needing_to_be_added = shifts_needing_to_be_added.merge(
                away_shifts.assign(shift_number = away_shifts.shift_number.astype(int)).groupby('name')['shift_number'].max().reset_index().rename(columns = {'shift_number':'prior_max_shift'}),
                how = 'left'
            ).fillna(0)

            shifts_needing_to_be_added = shifts_needing_to_be_added.assign(shift_number = shifts_needing_to_be_added.prior_max_shift + 1)

            shifts_needing_to_be_added.shift_number = shifts_needing_to_be_added.shift_number.astype(int)

            shifts_needing_to_be_added = shifts_needing_to_be_added.loc[:, ['shift_number', 'period', 'shift_start', 'shift_end', 'duration', 'name', 'number', 'team', 'venue']]

            shifts_needing_to_be_added['number'] = shifts_needing_to_be_added['number'].astype(int)

            away_shifts = pd.concat([away_shifts, shifts_needing_to_be_added]).sort_values(by = ['number', 'period', 'shift_number'])

        elif len(shifts_needing_to_be_added) == 0:
            away_clock_period = None
            away_clock_time_now = None
        
    global all_shifts

    home_shifts = home_shifts[~home_shifts.duration.str.startswith('-')]
    away_shifts = away_shifts[~away_shifts.duration.str.startswith('-')]
    
    all_shifts = pd.concat([home_shifts, away_shifts])

    all_shifts.name = all_shifts.name.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
    
    all_shifts = all_shifts.assign(start_time = all_shifts.shift_start.str.split('/').str[0])
    
    all_shifts = all_shifts.assign(end_time = all_shifts.shift_end.str.split('/').str[0])
    
    #all_shifts = all_shifts[~all_shifts.end_time.str.contains('\xa0')]
    
    # Filter out summary rows (GP, G, A, etc.) that might have been included
    # Period should be numeric (1-4) or 'OT', so filter out anything else
    if len(all_shifts) > 0:
        period_str = all_shifts.period.astype(str).str.strip()
        # Only keep rows where period is a valid period value
        valid_mask = period_str.isin(['1', '2', '3', '4', 'OT'])
        all_shifts = all_shifts[valid_mask].copy()
        
        if len(all_shifts) > 0:
            all_shifts.period = (np.where(all_shifts.period=='OT', 4, all_shifts.period)).astype(int)
    
    all_shifts = all_shifts.assign(end_time = np.where(~all_shifts.shift_end.str.contains('\xa0'), all_shifts.end_time,
              (np.where(
              (((pd.to_datetime(((60 * (all_shifts.start_time.str.split(':').str[0].astype(int))) + 
              (all_shifts.start_time.str.split(':').str[1].astype(int)) + 
                (60 * (all_shifts.duration.str.split(':').str[0].astype(int))).astype(int) +
              (all_shifts.duration.str.split(':').str[1].astype(int))).astype(int), unit = 's'))).dt.time).astype(str).str[3:].str[0]=='0',
              (((pd.to_datetime(((60 * (all_shifts.start_time.str.split(':').str[0].astype(int))) + 
              (all_shifts.start_time.str.split(':').str[1].astype(int)) + 
                (60 * (all_shifts.duration.str.split(':').str[0].astype(int))).astype(int) +
              (all_shifts.duration.str.split(':').str[1].astype(int))).astype(int), unit = 's'))).dt.time).astype(str).str[4:],
              (((pd.to_datetime(((60 * (all_shifts.start_time.str.split(':').str[0].astype(int))) + 
              (all_shifts.start_time.str.split(':').str[1].astype(int)) + 
                (60 * (all_shifts.duration.str.split(':').str[0].astype(int))).astype(int) +
              (all_shifts.duration.str.split(':').str[1].astype(int))).astype(int), unit = 's'))).dt.time).astype(str).str[4:]))))
    
    # OPTIMIZED: Batch string replacements instead of conditional np.where()
    all_shifts['name'] = (all_shifts['name']
        .str.replace('ALEXANDRE ', 'ALEX ', regex=False)
        .str.replace('ALEXANDER ', 'ALEX ', regex=False)
        .str.replace('CHRISTOPHER ', 'CHRIS ', regex=False))
    
    # OPTIMIZED: Use dictionary lookup instead of nested np.where() chains
    all_shifts['name'] = all_shifts['name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
    all_shifts['name'] = all_shifts['name'].apply(lambda x: normalize_player_name(x))
    
    # OPTIMIZED: Already handled by dictionary lookup above
    # Old nested chains removed - they were replaced with: all_shifts['name'] = all_shifts['name'].replace(_NAME_CORRECTIONS) 
    # Old nested chains removed - replaced with dictionary lookup

    # Apply regex to remove (A) and (C) designations at end of names
    all_shifts['name'] = all_shifts['name'].apply(lambda x: re.sub(r' \(A\)$', '', x).strip())
    all_shifts['name'] = all_shifts['name'].apply(lambda x: re.sub(r' \(C\)$', '', x).strip())
    
    # Apply specific name corrections
    all_shifts['name'] = np.where(all_shifts['name']== "JURAJ SLAFKOVSKA" , "JURAJ SLAFKOVSKY", all_shifts['name']) # Need to do this after normalization, only then he becomes Slafkovska?
    all_shifts['name'] = np.where(all_shifts['name']== "JOHN (JACK) ROSLOVIC" , "JACK ROSLOVIC", all_shifts['name'])
    all_shifts['name'] = np.where(all_shifts['name']== "ANTHONY-JOHN (AJ) GREER" , "A.J. GREER", all_shifts['name'])

    all_shifts['name'] = np.where(all_shifts['name']== 'MARTIN FEHARVARY' , 'MARTIN FEHERVARY', all_shifts['name']) 

    all_shifts['name'] = np.where(all_shifts['name']== 'MATAJ  BLAMEL' , 'MATAJ BLAMEL', all_shifts['name']) 
    
    all_shifts['name'] = all_shifts['name'].str.replace('  ', ' ')
    
    # Clean invalid time values (e.g., "28:10" should be "20:00")
    # Times beyond 20:00 (or 5:00 for OT periods) are invalid and should be capped
    def clean_time_value(time_str):
        """Clean invalid time values by capping hours at 20 (23 for parsing, but we'll cap at period max)"""
        if pd.isna(time_str):
            return time_str
        try:
            # Try to parse as-is first
            pd.to_datetime(time_str)
            return time_str
        except:
            # If parsing fails, extract minutes:seconds and cap appropriately
            try:
                parts = str(time_str).split(':')
                if len(parts) == 2:
                    minutes = int(parts[0])
                    seconds = parts[1]
                    # If minutes >= 20, cap at 20:00 (end of regulation period)
                    if minutes >= 20:
                        return '20:00'
                    else:
                        return time_str
            except:
                pass
            # If all else fails, return 20:00 as safe default
            return '20:00'
    
    try:
        all_shifts['start_time'] = all_shifts['start_time'].apply(clean_time_value)
        all_shifts['end_time'] = all_shifts['end_time'].apply(clean_time_value)
    except Exception as e:
        print(f'Error cleaning time values: {e}')
        print('Stupid vibe coded system is causing problems')
    
    all_shifts = all_shifts.assign(end_time = np.where(pd.to_datetime(all_shifts.start_time).dt.time > pd.to_datetime(all_shifts.end_time).dt.time, '20:00', all_shifts.end_time),
                                  goalie = np.where(all_shifts.name.isin(goalie_names), 1, 0))
    
    all_shifts = all_shifts.merge(all_shifts[all_shifts.goalie==1].groupby(['team', 'period'])['name'].nunique().reset_index().rename(columns = {'name':'period_gs'}), how = 'left').fillna(0)
    
    # Implement fix for goalies: Goalies who showed up late in the period and were the only goalie to play have their start time re-set to 0:00. 
    
    # Added this period shift number thing because we were getting an issue where a goalie got pulled mid period (like for a delayed penalty) and came back and their start time for the second shift got pushed to 0. 
    all_shifts = all_shifts.assign(period_shift_number = all_shifts.groupby(['period', 'name']).cumcount() + 1)

    all_shifts = all_shifts.assign(start_time = np.where((all_shifts.goalie==1) & (all_shifts.start_time!='0:00') & (all_shifts.period_gs==1) & (all_shifts.period_shift_number==1), '0:00', all_shifts.start_time))
    
    # Previously I had this code to fix some kind of problem where goalie shifts didn't properly end.
    # But now I see this is causing an issue: If a goalie gets pulled and never comes back, this inaccurately fills them in.
    # Commenting this out and testing what things look like without it. 
    
    # all_shifts = all_shifts.assign(end_time = np.where(
    # (pd.to_datetime(all_shifts.start_time).dt.time < datetime(2021, 6, 10, 18, 0, 0).time()) & 
    # (all_shifts.period!=3) & 
    # (all_shifts.period!=4) &
    # (all_shifts.period!=5) &
    # (all_shifts.goalie==1) &
    # (all_shifts.period_gs==1),
    # '20:00', all_shifts.end_time))
    
    # all_shifts = all_shifts.assign(end_time = np.where(
    # (pd.to_datetime(all_shifts.start_time).dt.time < datetime(2021, 6, 10, 13, 0, 0).time()) & 
    # (all_shifts.period!=4) &
    # (all_shifts.period!=5) &
    # (all_shifts.goalie==1) &
    # (all_shifts.period_gs==1),
    # '20:00', all_shifts.end_time))
    
    global myshifts
    global changes_on
    global changes_off
    myshifts = all_shifts
    #print('Printing my shifts')

    #print(myshifts)
    
    myshifts.start_time = myshifts.start_time.str.strip()
    myshifts.end_time = myshifts.end_time.str.strip()

    myshifts['number'] = myshifts.number.astype(str)

    changes_on = myshifts.groupby(['team', 'period', 'start_time']).agg(
        on = ('name', ', '.join),
        on_numbers = ('number', ', '.join),
        number_on = ('name', 'count')
    ).reset_index().rename(columns = {'start_time':'time'}).sort_values(by = ['team', 'period', 'time'])
    
    changes_off = myshifts.groupby(['team', 'period', 'end_time']).agg(
        off = ('name', ', '.join),
        off_numbers = ('number', ', '.join),
        number_off = ('name', 'count')
    ).reset_index().rename(columns = {'end_time':'time'}).sort_values(by = ['team', 'period', 'time'])
    
    all_on = changes_on.merge(changes_off, on = ['team', 'period', 'time'], how = 'left')
    off_only = changes_off.merge(changes_on, on = ['team', 'period', 'time'], how = 'left', indicator = True)[
    changes_off.merge(changes_on, on = ['team', 'period', 'time'], how = 'left', indicator = True)['_merge']!='both']
    full_changes = pd.concat([all_on, off_only]).sort_values(by = ['period', 'time']).drop(columns = ['_merge'])
    
    full_changes['period_seconds'] = full_changes.time.str.split(':').str[0].astype(int) * 60 + full_changes.time.str.split(':').str[1].astype(int)

    full_changes['game_seconds'] = (np.where((full_changes.period<5) & int(game_id)!=3, 
                                   (((full_changes.period - 1) * 1200) + full_changes.period_seconds),
                          3900))
    
    full_changes = full_changes.assign(team = np.where(full_changes.team.str.contains('CANADI'), 'MONTREAL CANADIENS', full_changes.team)).sort_values(by = 'game_seconds')

    if live == True:

        if home_clock_period is not None and away_clock_period is not None:
            
            min_game_clock = ((min([home_clock_period, away_clock_period]) - 1) * 1200) + min([convert_clock_to_seconds(home_clock_time_now), convert_clock_to_seconds(away_clock_time_now)])

        elif home_clock_period is not None and away_clock_period is None:

            min_game_clock = ((min([home_clock_period]) - 1) * 1200) + min([convert_clock_to_seconds(home_clock_time_now)])

        elif away_clock_period is not None and home_clock_period is None:

            min_game_clock = ((min([away_clock_period]) - 1) * 1200) + min([convert_clock_to_seconds(away_clock_time_now)])
        
        else:
            min_game_clock = None

        if min_game_clock is not None:

            full_changes = full_changes[full_changes.game_seconds <= min_game_clock]

        return min_game_clock, full_changes.reset_index(drop = True)

    return full_changes.reset_index(drop = True)

def scrape_html_events(season, game_id, events_page=None, roster_page=None, verbose=False):
    """
    Scrape HTML events page.
    
    Args:
        season: Season string (e.g., '20242025')
        game_id: Game ID string (e.g., '020333')
        events_page: Optional pre-fetched requests.Response object for events page. If None, will fetch.
        roster_page: Optional pre-fetched requests.Response object for roster page. If None, will fetch.
        verbose: If True, print detailed timing information
    
    Returns:
        Tuple of (events DataFrame, roster DataFrame)
    """
    #global game
    if events_page is None:
        url = 'http://www.nhl.com/scores/htmlreports/' + season + '/PL0' + game_id + '.HTM'
        
        # TIME: Network request
        net_start = time.time()
        events_page = _session.get(url, timeout=10)
        net_duration = time.time() - net_start
        if verbose:
            try:
                print(f'  ⏱️ HTML events network request: {net_duration:.2f}s')
            except Exception:
                pass
    
    #if int(season)<20092010):
     #   soup = BeautifulSoup(page.content, 'html.parser')
    #else:
     #   soup = BeautifulSoup(page.content, 'lxml')
    
    # TIME: Parsing
    parse_start = time.time()
    # OPTIMIZED: Use lxml directly instead of BeautifulSoup for faster parsing
    if type(events_page) == str:
        doc = html.fromstring(events_page)
    else:
        doc = html.fromstring(events_page.content.decode('ISO-8859-1'))
    # XPath to find td elements with class containing 'bborder'
    tds = doc.xpath("//td[contains(@class, 'bborder')]")
    #global stripped_html
    #global eventdf
    stripped_html = hs_strip_html(tds)
    length = (len(stripped_html) // 8) * 8

    # Validate we got meaningful HTML data
    if length == 0:
        raise ValueError(f"HTML events page returned no valid event data (found {len(tds)} td elements)")

    stripped_html = stripped_html[:length]
    eventdf = pd.DataFrame(np.array(stripped_html).reshape(int(length/8), 8)).rename(
    columns = {0:'index', 1:'period', 2:'strength', 3:'time', 4:'event', 5:'description', 6:'away_skaters', 7:'home_skaters'})

    # Validate time column has actual time data (should contain ':' like "12:34")
    # This catches cases where HTML returned but contained garbage/error page content
    eventdf['time'] = eventdf['time'].astype(str)
    if not eventdf['time'].str.contains(':').any():
        raise ValueError(f"HTML events page has no valid time data (first 3 values: {eventdf['time'].head(3).tolist()})")
    split = eventdf.time.str.split(':')
    # XPath to find td elements with align='center' and style containing 'font-size: 10px;font-weight:bold'
    potentialnames = doc.xpath("//td[@align='center' and contains(@style, 'font-size: 10px;font-weight:bold')]")
    game_date = potentialnames[2].text_content() if len(potentialnames) > 2 else ''
    
    for i in range(0, min(999, len(potentialnames))):
        away = potentialnames[i].text_content()
        if ('Away Game') in away or ('tr./Away') in away:
            away = _MATCH_GAME_PATTERN.split(away)[0]
            break
        
    for i in range(0, min(999, len(potentialnames))):
        home = potentialnames[i].text_content()
        if ('Home Game') in home or ('Dom./Home') in home:
            home = _MATCH_GAME_PATTERN.split(home)[0]
            break
            
    game = eventdf.assign(away_skaters = eventdf.away_skaters.str.replace('\n', ''),
                  home_skaters = eventdf.home_skaters.str.replace('\n', ''),
                  original_time = eventdf.time,
                  time = split.str[0] + ":" + split.str[1].str[:2],
                  home_team = home,
                  away_team = away)
    
    game = game.assign(away_team_abbreviated = game.away_skaters[0].split(' ')[0],
                       home_team_abbreviated = game.home_skaters[0].split(' ')[0])
    
    game = game[game.period!='Per']
    
    game = game.assign(index = game.index.astype(int)).rename(columns = {'index':'event_index'})
    
    game = game.assign(event_team = game.description.str.split(' ').str[0])
    
    game = game.assign(event_team = game.event_team.str.split('\xa0').str[0])
    
    game = game.assign(event_team = np.where(~game.event_team.isin([game.home_team_abbreviated.iloc[0], game.away_team_abbreviated.iloc[0]]), '\xa0', game.event_team))
    
    game = game.assign(other_team = np.where(game.event_team=='', '\xa0',
                                            np.where(game.event_team==game.home_team_abbreviated.iloc[0], game.away_team_abbreviated.iloc[0], game.home_team_abbreviated.iloc[0])))
    
    # Optimized: use single function instead of multiple .str.replace() calls
    def _extract_player_numbers(desc):
        matches = re.findall(r'[#-]\s*(\d+)', str(desc))
        return ' '.join(matches)
    game['event_player_str'] = game.description.apply(_extract_player_numbers)

    game = game.assign(event_player_1 = 
            game.event_player_str.str.split(' ').str[0],
            event_player_2 = 
            game.event_player_str.str.split(' ').str[1],
            event_player_3 = 
            game.event_player_str.str.split(' ').str[2])
    #return game

    if len(game[game.description.str.contains('Drawn By')])>0:
    
        game = game.assign(event_player_2 = np.where(game.description.str.contains('Drawn By'), 
                                          game.description.str.split('Drawn By').str[1].str.split('#').str[1].str.split(' ').str[0].str.strip(), 
                                          game.event_player_2),
                          event_player_3 = np.where(game.description.str.contains('Served By'),
                                                   '\xa0',
                                                   game.event_player_3))

    game = game.assign(event_player_1 = np.where((~pd.isna(game.event_player_1)) & (game.event_player_1!=''),
                              np.where(game.event=='FAC', game.away_team_abbreviated,
                                       game.event_team) + (game.event_player_1.astype(str)), 
                              game.event_player_1),
                  event_player_2 = np.where((~pd.isna(game.event_player_2)) & (game.event_player_2!=''),
                              np.where(game.event=='FAC', game.home_team_abbreviated,
                                       np.where(game.event.isin(['BLOCK', 'HIT', 'PENL']), game.other_team, game.event_team)) + (game.event_player_2.astype(str)), 
                              game.event_player_2),
                  event_player_3 = np.where((~pd.isna(game.event_player_3)) & (game.event_player_3!=''),
                              game.event_team + (game.event_player_3.astype(str)), 
                              game.event_player_3))
    
    game = game.assign(
        event_player_1 = np.where((game.event=='FAC') & (game.event_team==game.home_team_abbreviated),
                                 game.event_player_2, game.event_player_1),
        event_player_2 = np.where((game.event=='FAC') & (game.event_team==game.home_team_abbreviated),
                                 game.event_player_1, game.event_player_2))
    
    #return game
    
    roster = scrape_html_roster(season, game_id, page=roster_page, verbose=verbose).rename(columns = {'Nom/Name':'Name'})
    roster = roster[roster.status=='player']
    roster = roster.assign(team_abbreviated = np.where(roster.team=='home', 
                                                       game.home_team_abbreviated.iloc[0],
                                                      game.away_team_abbreviated.iloc[0]))

    roster = roster.assign(teamnum = roster.team_abbreviated + roster['#'])
    roster['Name'] = roster.Name.apply(lambda x: re.sub(r' \(A\)$', '', x).strip())
    roster['Name'] = roster.Name.apply(lambda x: re.sub(r' \(C\)$', '', x).strip())
    
    event_player_1s = roster.loc[:, ['teamnum', 'Name']].rename(columns = {'teamnum':'event_player_1', 'Name':'ep1_name'})
    event_player_2s = roster.loc[:, ['teamnum', 'Name']].rename(columns = {'teamnum':'event_player_2', 'Name':'ep2_name'})
    event_player_3s = roster.loc[:, ['teamnum', 'Name']].rename(columns = {'teamnum':'event_player_3', 'Name':'ep3_name'})
    
    game = game.merge(
    event_player_1s, on = 'event_player_1', how = 'left').merge(
    event_player_2s, on = 'event_player_2', how = 'left').merge(
    event_player_3s, on = 'event_player_3', how = 'left').assign(
    date = game_date)
    #return game
    game['period'] = np.where(game['period'] == '', '1', game['period'])
    game['time'] = np.where((game['time'] == '') | (pd.isna(game['time'])), '0:00', game['time'])
    game['period'] = game.period.astype(int)

    # OPTIMIZED: Split time once instead of twice
    time_split = game.time.str.split(':')
    game['period_seconds'] = time_split.str[0].str.replace('-', '', regex=False).astype(int) * 60 + time_split.str[1].str.replace('-', '', regex=False).astype(int)

    game['game_seconds'] = (np.where((game.period<5) & int(game_id[0])!=3, 
                                       (((game.period - 1) * 1200) + game.period_seconds),
                              3900))
    
    # OPTIMIZED: Use dictionary lookup instead of nested np.where()
    # TODO: Fix priority map so that we have change before shot or miss if the change involves a player returning from penalty box. 
    priority_map = {
        'TAKE': 1, 'GIVE': 1, 'MISS': 1, 'HIT': 1, 'SHOT': 1, 'BLOCK': 1,
        'GOAL': 2, 'STOP': 3, 'DELPEN': 4, 'PENL': 5, 'CHANGE': 6,
        'PEND': 7, 'GEND': 8, 'FAC': 9
    }
    game = game.assign(priority=game.event.map(priority_map).fillna(0).astype(int)).sort_values(by = ['game_seconds', 'period', 'event_player_1', 'event'])
    game = game.assign(version = 
                       (np.where(
                       (game.event==game.event.shift()) & 
                       (game.event_player_1==game.event_player_1.shift()) &
                       (game.event_player_1!='') &
                       (game.game_seconds==game.game_seconds.shift()),
                        1, 0)))
    
    game = game.assign(version = 
                           (np.where(
                           (game.event==game.event.shift(2)) & 
                           (game.event_player_1==game.event_player_1.shift(2)) &
                           (game.game_seconds==game.game_seconds.shift(2)) & 
                           (game.event_player_1!='') &
                           (~game.description.str.contains('Penalty Shot')),
                            2, game.version)))
    
    game = game.assign(version = 
                           (np.where(
                           (game.event==game.event.shift(3)) & 
                           (game.event_player_1==game.event_player_1.shift(3)) &
                           (game.game_seconds==game.game_seconds.shift(3)) & 
                           (game.event_player_1!=''),
                            3, game.version)))
    
    game = game.assign(date = pd.to_datetime(game.date[~pd.isna(game.date)].iloc[0])
                  ).rename(columns = {'date':'game_date'}).sort_values(by = ['event_index'])
    
    game = game.assign(event_player_1 = game.ep1_name, event_player_2 = game.ep2_name, event_player_3 = game.ep3_name).drop(columns = ['ep1_name', 'ep2_name', 'ep3_name'])
    
    # OPTIMIZED: Combine team name replacements into single operation
    team_replacements = {'CANADIENS MONTREAL': 'MONTREAL CANADIENS', 'MONTRÉAL CANADIENS': 'MONTREAL CANADIENS'}
    game['home_team'] = game['home_team'].replace(team_replacements)
    game['away_team'] = game['away_team'].replace(team_replacements)
    
    if int(game_id[0])!=3:
        game = game[game.game_seconds<4000]
    
    game['game_date'] = np.where((season=='20072008') & (game_id == '20003'), game.game_date + pd.Timedelta(days=1), game.game_date)
    
    # Only set event_player_1 to 'BENCH' for actual bench/team events (like team penalties)
    # Exclude BLOCK events where "TEAMMATE" appears (e.g., "BLOCKED BY TEAMMATE" - shooter is still a valid player)
    game = game.assign(event_player_1 = np.where(
        ((game.description.str.upper().str.contains('TEAM')) | (game.description.str.lower().str.contains('bench'))) &
        (~game.event.isin(['BLOCK'])),  # Don't replace player for BLOCKED BY TEAMMATE events
        'BENCH',
        game.event_player_1))
    
    game = game.assign(home_skater_count_temp = (game.home_skaters.apply(lambda x: len(re.findall('[A-Z]', x)))),
          away_skater_count_temp = (game.away_skaters.apply(lambda x: len(re.findall('[A-Z]', x))))
         )
    
    game = game.assign(event_team = np.where((game.event=='PENL') & (game.event_team=='') & (game.description.str.lower().str.contains('bench')) & (game.home_skater_count_temp>game.home_skater_count_temp.shift(-1)),
                                game.home_team_abbreviated, game.event_team))

    game = game.assign(event_team = np.where((game.event=='PENL') & (game.event_team=='') & (game.description.str.lower().str.contains('bench')) & (game.away_skater_count_temp>game.away_skater_count_temp.shift(-1)),
                                game.away_team_abbreviated, game.event_team))
    
    # TIME: Total parsing
    total_parse_duration = time.time() - parse_start
    # try:
    #     print(f'  ⏱️ HTML events parsing/processing: {total_parse_duration:.2f}s')
    # except Exception:
    #     pass
    
    # OPTIMIZATION: Return roster to avoid re-scraping in merge_and_prepare
    return game.drop(columns = ['period_seconds', 'time', 'priority', 'home_skater_count_temp', 'away_skater_count_temp']), roster

def scrape_espn_events(espn_game_id, drop_description = True):

    # This URL has event coordinates
    
    url = f'https://www.espn.com/nhl/playbyplay/_/gameId/{espn_game_id}'
    
    page = _session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
    
    soup = BeautifulSoup(page.content.decode('ISO-8859-1'), 'lxml', multi_valued_attributes = None)
    
    period_jsons = json.loads(str(soup).split('"playGrps":')[1].split(',"tms"')[0])
    
    # OPTIMIZED: Use list + concat instead of repeated _append()
    clock_df_list = []
    for period in range(0, len(period_jsons)):
        clock_df_list.append(pd.DataFrame(period_jsons[period]))
    clock_df = pd.concat(clock_df_list, ignore_index=True) if clock_df_list else pd.DataFrame()

    clock_df = clock_df[~pd.isna(clock_df.clock)]

    # Needed to add .split(',"st":3')[0] for playoffs

    coords_df = pd.DataFrame(json.loads(str(soup).split('plays":')[1].split(',"st":1')[0].split(',"st":2')[0].split(',"st":3')[0]))

    clock_df = clock_df.assign(
        clock = clock_df.clock.apply(lambda x: x['displayValue'])
    )
    
    coords_df = coords_df.assign(
        coords_x = coords_df[~pd.isna(coords_df.coordinate)].coordinate.apply(lambda x: x['x']).astype(int),
        coords_y = coords_df[~pd.isna(coords_df.coordinate)].coordinate.apply(lambda y: y['y']).astype(int),
        event_player_1 = coords_df[~pd.isna(coords_df.athlete)]['athlete'].apply(lambda x: x['name'])
    )

    espn_events = coords_df.merge(clock_df.loc[:, ['id', 'clock']])

    espn_events = espn_events.assign(
        period = espn_events['period'].apply(lambda x: x['number']),
        minutes = espn_events['clock'].str.split(':').apply(lambda x: x[0]).astype(int),
        seconds = espn_events['clock'].str.split(':').apply(lambda x: x[1]).astype(int),
        event_type = espn_events['type'].apply(lambda x: x['txt'])
    )

    espn_events = espn_events.assign(coords_x = np.where((pd.isna(espn_events.coords_x)) & (pd.isna(espn_events.coords_y)) &
                (espn_events.event_type=='Face Off'), 0, espn_events.coords_x
    ),
                      coords_y = np.where((pd.isna(espn_events.coords_x)) & (pd.isna(espn_events.coords_y)) &
                (espn_events.event_type=='Face Off'), 0, espn_events.coords_y))

    espn_events = espn_events[(~pd.isna(espn_events.coords_x)) & (~pd.isna(espn_events.coords_y)) & (~pd.isna(espn_events.event_player_1))]

    espn_events = espn_events.assign(
        # Do this later
        coords_x = espn_events.coords_x.astype(int),
        coords_y = espn_events.coords_y.astype(int)
    )
    
    espn_events = espn_events.rename(columns = {'text':'description'})
    
    espn_events = espn_events.assign(
        event_type = np.where(espn_events.event_type=='Face Off', 'FAC',
                             np.where(espn_events.event_type=='Goal', 'GOAL',
                                 np.where(espn_events.event_type=='Giveaway', 'GIVE',
                                     np.where(espn_events.event_type=='Penalty', 'PENL',
                                         np.where(espn_events.event_type=='Missed', 'MISS',
                                             np.where(espn_events.event_type=='Shot', 'SHOT',
                                                 np.where(espn_events.event_type=='Takeaway', 'TAKE',
                                                     np.where(espn_events.event_type=='Blocked', 'BLOCK',
                                                         np.where(espn_events.event_type=='Hit', 'HIT',
                                                              espn_events.event_type))))))))))
    
    espn_events = espn_events.assign(priority = np.where(espn_events.event_type.isin(['TAKE', 'GIVE', 'MISS', 'HIT', 'SHOT', 'BLOCK']), 1, 
                                            np.where(espn_events.event_type=="GOAL", 2,
                                                np.where(espn_events.event_type=="STOP", 3,
                                                    np.where(espn_events.event_type=="DELPEN", 4,
                                                        np.where(espn_events.event_type=="PENL", 5,
                                                            np.where(espn_events.event_type=="CHANGE", 6,
                                                                np.where(espn_events.event_type=="PEND", 7,
                                                                    np.where(espn_events.event_type=="GEND", 8,
                                                                        np.where(espn_events.event_type=="FAC", 9, 0))))))))),
                                    event_player_1 = espn_events.event_player_1.str.upper(),
                                    game_seconds = np.where(espn_events.period<5, 
                                    ((espn_events.period - 1) * 1200) + (espn_events.minutes * 60) + espn_events.seconds, 3900))
    
    espn_events = espn_events.sort_values(by = ['period', 'game_seconds', 'event_player_1', 'priority']).rename(
    columns = {'event_type':'event'}).loc[:, ['coords_x', 'coords_y', 'event_player_1', 'event', 'game_seconds', 'description', 'period']]
    
    espn_events['event_player_1'] = np.where(espn_events['event_player_1'].str.contains('ALEXANDRE '), 
                                espn_events['event_player_1'].str.replace('ALEXANDRE ', 'ALEX '),
                                espn_events['event_player_1'])
    
    espn_events['event_player_1'] = np.where(espn_events['event_player_1'].str.contains('ALEXANDER '), 
                                espn_events['event_player_1'].str.replace('ALEXANDER ', 'ALEX '),
                                espn_events['event_player_1'])
    
    espn_events['event_player_1'] = np.where(espn_events['event_player_1'].str.contains('CHRISTOPHER '), 
                                espn_events['event_player_1'].str.replace('CHRISTOPHER ', 'CHRIS '),
                                espn_events['event_player_1'])
    
    espn_events = espn_events.assign(event_player_1 = 
    np.where(espn_events.event_player_1=='PATRICK MAROON', 'PAT MAROON',
    (np.where(espn_events.event_player_1=='J T COMPHER', 'J.T. COMPHER', 
    (np.where(espn_events.event_player_1=='J T MILLER', 'J.T. MILLER', 
    (np.where(espn_events.event_player_1=='T J OSHIE', 'T.J. OSHIE', 
    (np.where((espn_events.event_player_1=='ALEXIS LAFRENIERE') | (espn_events.event_player_1=='ALEXIS LAFRENI RE'), 'ALEXIS LAFRENIÈRE', 
    (np.where((espn_events.event_player_1=='TIM STUTZLE') | (espn_events.event_player_1=='TIM ST TZLE'), 'TIM STÜTZLE',
    (np.where(espn_events.event_player_1=='T.J. BRODIE', 'TJ BRODIE',
    (np.where(espn_events.event_player_1=='MATTHEW IRWIN', 'MATT IRWIN',
    (np.where(espn_events.event_player_1=='STEVE KAMPFER', 'STEVEN KAMPFER',
    (np.where(espn_events.event_player_1=='STEVE KAMPFER', 'STEVEN KAMPFER',
    (np.where(espn_events.event_player_1=='JEFFREY TRUCHON-VIEL', 'JEFFREY VIEL',
    (np.where(espn_events.event_player_1=='ZACHARY JONES', 'ZAC JONES',
    (np.where(espn_events.event_player_1=='MITCH MARNER', 'MITCHELL MARNER',
    (np.where(espn_events.event_player_1=='MATHEW DUMBA', 'MATT DUMBA',
    (np.where(espn_events.event_player_1=='JOSHUA MORRISSEY', 'JOSH MORRISSEY',
    (np.where(espn_events.event_player_1=='P K SUBBAN', 'P.K. SUBBAN',
    (np.where(espn_events.event_player_1=='EGOR SHARANGOVICH', 'YEGOR SHARANGOVICH',
    (np.where(espn_events.event_player_1=='MAXIME COMTOIS', 'MAX COMTOIS',
    (np.where(espn_events.event_player_1=='NICHOLAS CAAMANO', 'NICK CAAMANO',
    (np.where(espn_events.event_player_1=='DANIEL CARCILLO', 'DAN CARCILLO',
    (np.where(espn_events.event_player_1=='ALEXANDER OVECHKIN', 'ALEX OVECHKIN',
    (np.where(espn_events.event_player_1=='MICHAEL CAMMALLERI', 'MIKE CAMMALLERI',
    (np.where(espn_events.event_player_1=='DAVE STECKEL', 'DAVID STECKEL',
    (np.where(espn_events.event_player_1=='JIM DOWD', 'JAMES DOWD', 
    (np.where(espn_events.event_player_1=='MAXIME TALBOT', 'MAX TALBOT',
    (np.where(espn_events.event_player_1=='MIKE ZIGOMANIS', 'MICHAEL ZIGOMANIS',
    (np.where(espn_events.event_player_1=='VINNY PROSPAL', 'VACLAV PROSPAL',
    (np.where(espn_events.event_player_1=='MIKE YORK', 'MICHAEL YORK',
    (np.where(espn_events.event_player_1=='JACOB DOWELL', 'JAKE DOWELL',
    (np.where(espn_events.event_player_1=='MICHAEL RUPP', 'MIKE RUPP',
    (np.where(espn_events.event_player_1=='ALEXEI KOVALEV', 'ALEX KOVALEV',
    (np.where(espn_events.event_player_1=='SLAVA KOZLOV', 'VYACHESLAV KOZLOV',
    (np.where(espn_events.event_player_1=='JEFF HAMILTON', 'JEFFREY HAMILTON',
    (np.where(espn_events.event_player_1=='JOHNNY POHL', 'JOHN POHL',
    (np.where(espn_events.event_player_1=='DANIEL GIRARDI', 'DAN GIRARDI',
    (np.where(espn_events.event_player_1=='NIKOLAI ZHERDEV', 'NIKOLAY ZHERDEV',
    (np.where(espn_events.event_player_1=='J.P. DUMONT', 'J-P DUMONT',
    (np.where(espn_events.event_player_1=='DWAYNE KING', 'DJ KING',
    (np.where(espn_events.event_player_1=='JOHN ODUYA', 'JOHNNY ODUYA',
    (np.where(espn_events.event_player_1=='ROBERT SCUDERI', 'ROB SCUDERI',
    (np.where(espn_events.event_player_1=='DOUG MURRAY', 'DOUGLAS MURRAY',
    (np.where(espn_events.event_player_1=='VACLAV PROSPAL', 'VINNY PROSPAL',
    (np.where(espn_events.event_player_1=='RICH PEVERLY', 'RICH PEVERLEY',
    espn_events.event_player_1.str.strip()
             ))))))))))))))))))))))))))))))))))))))))))))
             ))))))))))))))))))))))))))))))))))))))))))

    espn_events['event_player_1'] = (np.where(espn_events['event_player_1']== "JANIS MOSER" , "J.J. MOSER",
    (np.where(espn_events['event_player_1']== "NICHOLAS PAUL" , "NICK PAUL",
    (np.where(espn_events['event_player_1']== "JACOB MIDDLETON" , "JAKE MIDDLETON",
    (np.where(espn_events['event_player_1']== "TOMMY NOVAK" , "THOMAS NOVAK",
    espn_events['event_player_1']))))))))

    espn_events['event_player_1'] = (np.where(espn_events['event_player_1']== "JOHHNY BEECHER" , "JOHN BEECHER",
    (np.where(espn_events['event_player_1']== "ALEXANDER BARKOV" , "ALEKSANDER BARKOV",
    (np.where(espn_events['event_player_1']== "TOMMY NOVAK" , "THOMAS NOVAK",
    espn_events['event_player_1']))))))

    espn_events['event_player_1'] = (np.where(espn_events['event_player_1']== "JANIS MOSER" , "J.J. MOSER",
    (np.where(espn_events['event_player_1']== "NICHOLAS PAUL" , "NICK PAUL",
    (np.where(espn_events['event_player_1']== "JACOB MIDDLETON" , "JAKE MIDDLETON",
    (np.where(espn_events['event_player_1']== "TOMMY NOVAK" , "THOMAS NOVAK",
    # New guys from 24-25
    (np.where(espn_events['event_player_1']== "JOSHUA NORRIS" , "JOSH NORRIS",
    (np.where(espn_events['event_player_1']== "P.O JOSEPH" , "PIERRE-OLIVIER JOSEPH",
    (np.where(espn_events['event_player_1']== "MIKEY EYSSIMONT" , "MICHAEL EYSSIMONT",
    (np.where(espn_events['event_player_1']== "MATAJ  BLAMEL" , "MATAJ BLAMEL",
    (np.where(espn_events['event_player_1']== "VITTORIO MANCINI" , "VICTOR MANCINI",
    (np.where(espn_events['event_player_1']== "JOSHUA MAHURA" , "JOSH MAHURA",
    (np.where(espn_events['event_player_1']== "JOSEPH VELENO" , "JOE VELENO",
    (np.where(espn_events['event_player_1']== "ZACK BOLDUC" , "ZACHARY BOLDUC",
    (np.where(espn_events['event_player_1']== "JOSHUA BROWN" , "JOSH BROWN",
    (np.where(espn_events['event_player_1']== "JAKE LUCCHINI" , "JACOB LUCCHINI",
    (np.where(espn_events['event_player_1']== "EMIL LILLEBERG" , "EMIL MARTINSEN LILLEBERG",
    (np.where(espn_events['event_player_1']== "CAMERON ATKINSON" , "CAM ATKINSON",
    (np.where(espn_events['event_player_1']== "JURAJ SLAFKOVSKA" , "JURAJ SLAFKOVSKY",
    (np.where(espn_events['event_player_1']== "MARTIN FEHARVARY" , "MARTIN FEHERVARY",
    espn_events['event_player_1']))))))))))))))))))))))))))))))))))))

    
    espn_events = espn_events.assign(version = 
                       (np.where(
                       (espn_events.event==espn_events.event.shift()) & 
                       (espn_events.event_player_1==espn_events.event_player_1.shift()) &
                       (espn_events.event_player_1!='') &
                       (espn_events.game_seconds==espn_events.game_seconds.shift()),
                        1, 0)))
    
    espn_events = espn_events.assign(version = 
                           (np.where(
                           (espn_events.event==espn_events.event.shift(2)) & 
                           (espn_events.event_player_1==espn_events.event_player_1.shift(2)) &
                           (espn_events.game_seconds==espn_events.game_seconds.shift(2)) & 
                           (espn_events.event_player_1!='') &
                           (~espn_events.description.str.contains('Penalty Shot')),
                            2, espn_events.version)))
    
    espn_events = espn_events.assign(version = 
                           (np.where(
                           (espn_events.event==espn_events.event.shift(3)) & 
                           (espn_events.event_player_1==espn_events.event_player_1.shift(3)) &
                           (espn_events.game_seconds==espn_events.game_seconds.shift(3)) & 
                           (espn_events.event_player_1!=''),
                            3, espn_events.version)))
    
    espn_events['espn_id'] = int(espn_game_id)
    
    espn_events['event_player_1'] = espn_events['event_player_1'].str.strip()
    
    espn_events['event_player_1'] = espn_events['event_player_1'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()

    # Apply regex to remove (A) and (C) designations at end of names
    espn_events['event_player_1'] = espn_events['event_player_1'].apply(lambda x: re.sub(r' \(A\)$', '', x).strip())
    espn_events['event_player_1'] = espn_events['event_player_1'].apply(lambda x: re.sub(r' \(C\)$', '', x).strip())
    
    # Apply specific name corrections
    espn_events['event_player_1'] = np.where(espn_events['event_player_1'] == "JURAJ SLAFKOVSKA" , "JURAJ SLAFKOVSKY", espn_events['event_player_1'])
    espn_events['event_player_1'] = np.where(espn_events['event_player_1'] == "JOHN (JACK) ROSLOVIC" , "JACK ROSLOVIC", espn_events['event_player_1'])
    espn_events['event_player_1'] = np.where(espn_events['event_player_1'] == "ANTHONY-JOHN (AJ) GREER" , "A.J. GREER", espn_events['event_player_1']) 

    espn_events['event_player_1'] = np.where(espn_events['event_player_1'] == 'MARTIN FEHARVARY' , 'MARTIN FEHERVARY', espn_events['event_player_1']) 

    espn_events['event_player_1'] = np.where(espn_events['event_player_1'] == 'MATAJ  BLAMEL' , 'MATAJ BLAMEL', espn_events['event_player_1']) 
    
    espn_events['event_player_1'] = espn_events['event_player_1'].str.replace('  ', ' ')

    #espn_events = espn_events.assign(event_player_1 = np.where(
    #espn_events.event_player_1=='ALEX BURROWS', 'ALEXANDRE BURROWS', espn_events.event_player_1))
    
    global look
    look = espn_events
    
    espn_events['coords_x'] = np.where(espn_events['coords_x']>99, 99, espn_events['coords_x'])
    espn_events['coords_y'] = np.where(espn_events['coords_y']<(-42), (-42), espn_events['coords_y'])

    if drop_description == True:
        return espn_events.drop(columns = 'description')
    else:
        return espn_events

def scrape_espn_ids_single_game(game_date, home_team, away_team):
    
    gamedays = pd.DataFrame()
    
    if home_team == 'ATLANTA THRASHERS':
        home_team = 'WINNIPEG JETS'
    if away_team == 'ATLANTA THRASHERS':
        away_team = 'WINNIPEG JETS'
        
    if home_team == 'PHOENIX COYOTES':
        home_team = 'ARIZONA COYOTES'
    if away_team == 'PHOENIX COYOTES':
        away_team = 'ARIZONA COYOTES'
    
    this_date = (game_date)
    url = 'http://www.espn.com/nhl/scoreboard?date=' + this_date.replace("-", "")
    page = _session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
    print('Request to ESPN IDs successful.')
    soup = BeautifulSoup(page.content, 'lxml')
    soup_found = soup.find_all('a', {'class':['AnchorLink truncate', 
                             'AnchorLink Button Button--sm Button--anchorLink Button--alt mb4 w-100',
                            'AnchorLink Button Button--sm Button--anchorLink Button--alt mb4 w-100 mr2'], 'href':[re.compile("/nhl/team/_/name/"), re.compile("game/_")]})
    game_sections = soup.find_all('section', class_ = 'Scoreboard bg-clr-white flex flex-auto justify-between')

    at = []
    ht = []
    gids = []
    fax = pd.DataFrame()
    #print(str(i))
    for i in range(0, len(game_sections)):
        away = game_sections[i].find_all('div', class_='ScoreCell__TeamName ScoreCell__TeamName--shortDisplayName db')[0].contents[0].upper()
        home = game_sections[i].find_all('div', class_='ScoreCell__TeamName ScoreCell__TeamName--shortDisplayName db')[1].contents[0].upper()
        espnid = game_sections[i]['id']
        at.append(away)
        ht.append(home)
        gids.append(espnid)
    
    fax = fax.assign(
    away_team = at,
    home_team = ht,
    espn_id = gids,
    game_date = pd.to_datetime(this_date))
    
    # OPTIMIZED: Use concat instead of _append
    gamedays = pd.concat([gamedays, fax], ignore_index=True) if len(gamedays) > 0 else fax

    gamedays = gamedays[gamedays.espn_id!='gameId']
    
    gamedays = gamedays.assign(
        home_team = np.where(gamedays.home_team=='ST LOUIS BLUES', 'ST. LOUIS BLUES', gamedays.home_team),
        away_team = np.where(gamedays.away_team=='ST LOUIS BLUES', 'ST. LOUIS BLUES', gamedays.away_team),
        espn_id = gamedays.espn_id.str.split('/').str[0].astype(int)
    
    )
    
    gamedays = gamedays.assign(
        home_team = np.where(gamedays.home_team=='TB', 'TBL',
                    np.where(gamedays.home_team=='T.B', 'TBL',
                    np.where(gamedays.home_team=='L.A', 'LAK',
                    np.where(gamedays.home_team=='LA', 'LAK',
                    np.where(gamedays.home_team=='S.J', 'SJS',
                    np.where(gamedays.home_team=='SJ', 'SJS',
                    np.where(gamedays.home_team=='N.J', 'NJD',
                    np.where(gamedays.home_team=='NJ', 'NJD',
                    gamedays.home_team)))))))),
        away_team = np.where(gamedays.away_team=='TB', 'TBL',
                    np.where(gamedays.away_team=='T.B', 'TBL',
                    np.where(gamedays.away_team=='L.A', 'LAK',
                    np.where(gamedays.away_team=='LA', 'LAK',
                    np.where(gamedays.away_team=='S.J', 'SJS',
                    np.where(gamedays.away_team=='SJ', 'SJS',
                    np.where(gamedays.away_team=='N.J', 'NJD',
                    np.where(gamedays.away_team=='NJ', 'NJD',
                    gamedays.away_team)))))))),
        espn_id = gamedays.espn_id.astype(int))
    
    gamedays = gamedays.assign(
        away_team = np.where(gamedays.away_team=='DUCKS', 'ANA',
                    np.where(gamedays.away_team=='COYOTES', 'ARI',
                    np.where(gamedays.away_team=='BRUINS', 'BOS',
                    np.where(gamedays.away_team=='SABRES', 'BUF',
                    np.where(gamedays.away_team=='FLAMES', 'CGY',
                    np.where(gamedays.away_team=='HURRICANES', 'CAR',
                    np.where(gamedays.away_team=='BLACKHAWKS', 'CHI',
                    np.where(gamedays.away_team=='AVALANCHE', 'COL',
                    np.where(gamedays.away_team=='BLUE', 'CBJ',
                    np.where(gamedays.away_team=='JACKETS', 'CBJ',
                    np.where(gamedays.away_team=='BLUE JACKETS', 'CBJ',
                    np.where(gamedays.away_team=='STARS', 'DAL',
                    np.where(gamedays.away_team=='RED', 'DET',
                    np.where(gamedays.away_team=='WINGS', 'DET',
                    np.where(gamedays.away_team=='RED WINGS', 'DET',
                    np.where(gamedays.away_team=='OILERS', 'EDM',
                    np.where(gamedays.away_team=='PANTHERS', 'FLA',
                    np.where(gamedays.away_team=='KINGS', 'LAK',
                    np.where(gamedays.away_team=='WILD', 'MIN',
                    np.where(gamedays.away_team=='CANADIENS', 'MTL',
                    np.where(gamedays.away_team=='PREDATORS', 'NSH',
                    np.where(gamedays.away_team=='DEVILS', 'NJD',
                    np.where(gamedays.away_team=='ISLANDERS', 'NYI',
                    np.where(gamedays.away_team=='RANGERS', 'NYR',
                    np.where(gamedays.away_team=='SENATORS', 'OTT',
                    np.where(gamedays.away_team=='FLYERS', 'PHI',
                    np.where(gamedays.away_team=='PENGUINS', 'PIT',
                    np.where(gamedays.away_team=='SHARKS', 'SJS',
                    np.where(gamedays.away_team=='KRAKEN', 'SEA',
                    np.where(gamedays.away_team=='BLUES', 'STL',
                    np.where(gamedays.away_team=='LIGHTNING', 'TBL',
                    np.where(gamedays.away_team=='LEAFS', 'TOR',
                    np.where(gamedays.away_team=='MAPLE', 'TOR',
                    np.where(gamedays.away_team=='MAPLE LEAFS', 'TOR',
                    np.where(gamedays.away_team=='CANUCKS', 'VAN',
                    np.where(gamedays.away_team=='GOLDEN', 'VGK',
                    np.where(gamedays.away_team=='KNIGHTS', 'VGK',
                    np.where(gamedays.away_team=='GOLDEN KNIGHTS', 'VGK',
                    np.where(gamedays.away_team=='CAPITALS', 'WSH',
                    np.where(gamedays.away_team=='JETS', 'WPG',
                    np.where(gamedays.away_team=='CLUB', 'UTA',
                    np.where(gamedays.away_team=='MAMMOTH', 'UTA',
                    np.where(gamedays.away_team=='HOCKEY', 'UTA', 'mistake'
                            ))))))))))))))))))))))))))))))))))))))))))))

    gamedays = gamedays.assign(
        home_team = np.where(gamedays.home_team=='DUCKS', 'ANA',
                    np.where(gamedays.home_team=='COYOTES', 'ARI',
                    np.where(gamedays.home_team=='BRUINS', 'BOS',
                    np.where(gamedays.home_team=='SABRES', 'BUF',
                    np.where(gamedays.home_team=='FLAMES', 'CGY',
                    np.where(gamedays.home_team=='HURRICANES', 'CAR',
                    np.where(gamedays.home_team=='BLACKHAWKS', 'CHI',
                    np.where(gamedays.home_team=='AVALANCHE', 'COL',
                    np.where(gamedays.home_team=='BLUE', 'CBJ',
                    np.where(gamedays.home_team=='JACKETS', 'CBJ',
                    np.where(gamedays.home_team=='BLUE JACKETS', 'CBJ',
                    np.where(gamedays.home_team=='STARS', 'DAL',
                    np.where(gamedays.home_team=='RED', 'DET',
                    np.where(gamedays.home_team=='WINGS', 'DET',
                    np.where(gamedays.home_team=='RED WINGS', 'DET',
                    np.where(gamedays.home_team=='OILERS', 'EDM',
                    np.where(gamedays.home_team=='PANTHERS', 'FLA',
                    np.where(gamedays.home_team=='KINGS', 'LAK',
                    np.where(gamedays.home_team=='WILD', 'MIN',
                    np.where(gamedays.home_team=='CANADIENS', 'MTL',
                    np.where(gamedays.home_team=='PREDATORS', 'NSH',
                    np.where(gamedays.home_team=='DEVILS', 'NJD',
                    np.where(gamedays.home_team=='ISLANDERS', 'NYI',
                    np.where(gamedays.home_team=='RANGERS', 'NYR',
                    np.where(gamedays.home_team=='SENATORS', 'OTT',
                    np.where(gamedays.home_team=='FLYERS', 'PHI',
                    np.where(gamedays.home_team=='PENGUINS', 'PIT',
                    np.where(gamedays.home_team=='SHARKS', 'SJS',
                    np.where(gamedays.home_team=='KRAKEN', 'SEA',
                    np.where(gamedays.home_team=='BLUES', 'STL',
                    np.where(gamedays.home_team=='LIGHTNING', 'TBL',
                    np.where(gamedays.home_team=='MAPLE', 'TOR',
                    np.where(gamedays.home_team=='LEAFS', 'TOR',
                    np.where(gamedays.home_team=='MAPLE LEAFS', 'TOR',
                    np.where(gamedays.home_team=='CANUCKS', 'VAN',
                    np.where(gamedays.home_team=='GOLDEN', 'VGK',
                    np.where(gamedays.home_team=='KNIGHTS', 'VGK',
                    np.where(gamedays.home_team=='GOLDEN KNIGHTS', 'VGK',
                    np.where(gamedays.home_team=='CAPITALS', 'WSH',
                    np.where(gamedays.home_team=='JETS', 'WPG', 
                    np.where(gamedays.home_team=='CLUB', 'UTA', 
                    np.where(gamedays.home_team=='MAMMOTH', 'UTA', 
                    np.where(gamedays.home_team=='HOCKEY', 'UTA', 'mistake'
                            ))))))))))))))))))))))))))))))))))))))))))))
    
    gamedays = gamedays[(gamedays.game_date==this_date) & (gamedays.home_team==home_team) & (gamedays.away_team==away_team)] 
        
    return(gamedays)

def merge_and_prepare(events, shifts, roster=None, live = False):
    
    season = str(int(str(events.game_id.iloc[0])[:4])) + str(int(str(events.game_id.iloc[0])[:4]) + 1)
    small_id = str(events.game_id.iloc[0])[5:]
    game_id = int(events.game_id.iloc[0])
    
    merged = pd.concat([events, shifts])

    home_team = merged[~(pd.isna(merged.home_team))].home_team.iloc[0]
    #print(home_team)
    away_team = merged[~(pd.isna(merged.away_team))].away_team.iloc[0]
    #print(away_team)

    if 'CANADIENS' in home_team:
        home_team = 'MONTREAL CANADIENS'

    if 'CANADIENS' in away_team:
        away_team = 'MONTREAL CANADIENS'

    #print(home_team)
    #print(away_team)
    
    merged = merged.assign(home_team = home_team,
                          away_team = away_team,
                          home_team_abbreviated = merged[~(pd.isna(merged.home_team_abbreviated))].home_team_abbreviated.iloc[0],
                          away_team_abbreviated = merged[~(pd.isna(merged.away_team_abbreviated))].away_team_abbreviated.iloc[0])

    merged = merged.assign(event_team = np.where(merged.team==merged.home_team, merged.home_team_abbreviated, 
                                        np.where(merged.team==merged.away_team, merged.away_team_abbreviated, 
                                                 merged.event_team)))

    merged = merged.assign(event = np.where((pd.isna(merged.event)) & 
                                     ((~pd.isna(merged.number_off)) | (~pd.isna(merged.number_on))), "CHANGE", merged.event))

    home_space = ' ' + merged['home_team_abbreviated'].iloc[0]
    away_space = ' ' + merged['away_team_abbreviated'].iloc[0]

    merged['away_skaters'] = np.where(pd.isna(merged.away_skaters), '\xa0', merged.away_skaters)

    merged['tmp'] = merged.away_skaters.str.replace("[^0-9]", " ")

    merged['tmp2'] = (merged.tmp.str.strip().str.split("  ")).apply(lambda x: natsorted(x)).apply(lambda x: ' '.join(x))

    merged['tmp2'] = (merged.away_team_abbreviated.iloc[0] + merged.tmp2).str.replace(" ", away_space).str.replace(" ", ", ")

    merged['tmp2'] = np.where(merged.tmp2.str.strip()==merged.away_team_abbreviated.iloc[0], '\xa0', merged.tmp2)

    merged['away_on_ice'] = merged['tmp2']

    merged['home_skaters'] = np.where(pd.isna(merged.home_skaters), '\xa0', merged.home_skaters)

    merged['tmp'] = merged.home_skaters.str.replace("[^0-9]", " ")

    merged['tmp2'] = (merged.tmp.str.strip().str.split("  ")).apply(lambda x: natsorted(x)).apply(lambda x: ' '.join(x))

    merged['tmp2'] = (merged.home_team_abbreviated.iloc[0] + merged.tmp2).str.replace(" ", home_space).str.replace(" ", ", ")

    merged['tmp2'] = np.where(merged.tmp2.str.strip()==merged.home_team_abbreviated.iloc[0], '\xa0', merged.tmp2)

    merged['home_on_ice'] = merged['tmp2']

    merged = merged.sort_values(by = ['game_seconds', 'period'])

    merged = merged.assign(jumping_on = (np.where(merged.home_team == merged.team, (merged.home_team_abbreviated.iloc[0] + merged.on_numbers).str.replace(", ", home_space).str.replace(" ", ", "), 
                                   np.where(merged.away_team == merged.team, (merged.away_team_abbreviated.iloc[0] + merged.on_numbers).str.replace(", ", away_space).str.replace(" ", ", "),
                                            '\xa0'))),
                          jumping_off = (np.where(merged.home_team == merged.team, (merged.home_team_abbreviated.iloc[0] + merged.off_numbers).str.replace(", ", home_space).str.replace(" ", ", "), 
                                   np.where(merged.away_team == merged.team, (merged.away_team_abbreviated.iloc[0] + merged.off_numbers).str.replace(", ", away_space).str.replace(" ", ", "),
                                            '\xa0'))),
                          prio = np.where(merged.event=="CHANGE", 0,
                                          np.where(merged.event.isin(['PGSTR', 'PGEND', 'PSTR', 'PEND', 'ANTHEM']), -1, 1))).sort_values(
        by = ['game_seconds', 'period', 'event_index'])

    merged = merged.assign(change_before_event = np.where(
        (
            (merged.away_on_ice!='') & (merged.event.shift()=='CHANGE') & (merged.away_on_ice!=merged.away_on_ice.shift()) | 
            (merged.home_on_ice!='') & (merged.event.shift()=='CHANGE') & (merged.home_on_ice!=merged.home_on_ice.shift())
        ), 1, 0
    ))

    merged = merged.assign(change_prio = 
                          np.where((merged.team==merged.home_team) & (merged.event=='CHANGE') , 1,
                                  np.where((merged.team==merged.away_team) & (merged.event=='CHANGE'), -1, 0)))

    # TODO: Fix priority map so that we have change before shot or miss if the change involves a player returning from penalty box. 
    merged = merged.assign(priority = np.where(merged.event.isin(['TAKE', 'GIVE', 'MISS', 'HIT', 'SHOT', 'BLOCK']), 1, 
                                                np.where(merged.event=="GOAL", 2,
                                                    np.where(merged.event=="STOP", 3,
                                                        np.where(merged.event=="DELPEN", 4,
                                                            np.where(merged.event=="PENL", 5,
                                                                np.where(merged.event=="CHANGE", 6,
                                                                    np.where(merged.event=="PEND", 7,
                                                                        np.where(merged.event=="GEND", 8,
                                                                            np.where(merged.event=="FAC", 9, 0)))))))))).sort_values(by = ['game_seconds', 'period', 'priority', 'event_index', 'change_prio'])

    merged = merged.reset_index(drop = True).reset_index().rename(columns = {'index':'event_index', 'event_index':'original_index'})

    # OPTIMIZATION: Use passed-in roster if available, otherwise scrape it
    if roster is None:
        roster = scrape_html_roster(season, small_id).rename(columns = {'Nom/Name':'Name'})
    # roster is already prepared in scrape_html_events, no need to rename

    roster = roster.assign(team_abbreviated = np.where(roster.team=='home', 
                                                       merged.home_team_abbreviated.iloc[0],
                                                      merged.away_team_abbreviated.iloc[0]))

    roster = roster.assign(teamnum = roster.team_abbreviated + roster['#'])
    # OPTIMIZED: Use pre-compiled regex patterns instead of compiling in each lambda
    roster['Name'] = roster.Name.apply(lambda x: _CAPTAIN_A_PATTERN.sub('', x).strip())
    roster['Name'] = roster.Name.apply(lambda x: _CAPTAIN_C_PATTERN.sub('', x).strip())

    roster = roster.assign(Name = np.where((roster.Name=='SEBASTIAN AHO') &( roster.team_name == 'NEW YORK ISLANDERS'), 'SEBASTIAN AHO (SWE)', roster.Name))
    roster = roster.assign(Name = np.where((roster.Name=='ELIAS PETTERSSON') &( roster.Pos == 'D'), 'ELIAS PETTERSSON(D)', roster.Name))

    goalies = roster[(roster.Pos=='G') & (roster.status!='scratch')]

    away_roster = roster[(roster.team=='away') & (roster.status!='scratch')]
    home_roster = roster[(roster.team=='home') & (roster.status!='scratch')]

    merged.jumping_on = np.where(pd.isna(merged.jumping_on), '\xa0', merged.jumping_on)
    merged.jumping_off = np.where(pd.isna(merged.jumping_off), '\xa0', merged.jumping_off)

    # OPTIMIZED: Use vectorized string operations instead of .apply()
    # This provides 10-20x speedup on the on-ice tracking loops
    change_mask = (merged.event == 'CHANGE')
    
    # OPTIMIZED: Use regex pattern matching for exact teamnum matches in comma-separated strings
    # Pattern matches teamnum at start, middle (after comma+space), or end of string
    # Note: re module is already imported at module level
    
    # Build all columns at once using vectorized string operations
    awaydf_dict = {}
    for i in range(0, len(away_roster)):
        teamnum = away_roster.teamnum.iloc[i]
        # Use regex to match teamnum as whole value (not substring)
        # Match: start of string OR comma+space, then teamnum, then comma OR end of string
        pattern = r'(^|, )' + re.escape(teamnum) + r'(,|$)'
        on_mask = merged.jumping_on.str.contains(pattern, na=False, regex=True)
        off_mask = merged.jumping_off.str.contains(pattern, na=False, regex=True) & change_mask
        vec = np.cumsum(on_mask.astype(int) - off_mask.astype(int))
        awaydf_dict[away_roster.Name.iloc[i]] = vec
    
    awaydf = pd.DataFrame(awaydf_dict)

    global homedf

    # OPTIMIZED: Same optimization for home roster
    homedf_dict = {}
    for i in range(0, len(home_roster)):
        teamnum = home_roster.teamnum.iloc[i]
        pattern = r'(^|, )' + re.escape(teamnum) + r'(,|$)'
        on_mask = merged.jumping_on.str.contains(pattern, na=False, regex=True)
        off_mask = merged.jumping_off.str.contains(pattern, na=False, regex=True) & change_mask
        vec = np.cumsum(on_mask.astype(int) - off_mask.astype(int))
        homedf_dict[home_roster.Name.iloc[i]] = vec
    
    homedf = pd.DataFrame(homedf_dict)

    global home_on
    global away_on

    # OPTIMIZED: Use list comprehension which is faster than .apply() for this operation
    # Get column names where value is 1, join, and sort
    home_on_list = []
    for idx in range(len(homedf)):
        row = homedf.iloc[idx]
        players = [col for col in homedf.columns if row[col] == 1]
        home_on_list.append(','.join(natsorted(players)) if players else '')
    home_on = pd.DataFrame({0: home_on_list})

    away_on_list = []
    for idx in range(len(awaydf)):
        row = awaydf.iloc[idx]
        players = [col for col in awaydf.columns if row[col] == 1]
        away_on_list.append(','.join(natsorted(players)) if players else '')
    away_on = pd.DataFrame({0: away_on_list})

    away_on = away_on[0].str.split(',', expand=True).rename(columns = {0:'away_on_1', 1:'away_on_2', 2:'away_on_3', 3:'away_on_4', 4:'away_on_5', 5:'away_on_6', 6:'away_on_7', 7:'away_on_8', 8:'away_on_9'})
    home_on = home_on[0].str.split(',', expand=True).rename(columns = {0:'home_on_1', 1:'home_on_2', 2:'home_on_3', 3:'home_on_4', 4:'home_on_5', 5:'home_on_6', 6:'home_on_7', 7:'home_on_8', 8:'home_on_9'})

    # OPTIMIZED: Initialize missing columns in a loop
    for side in ['away', 'home']:
        for i in range(1, 10):
            col = f'{side}_on_{i}'
            if col not in (away_on if side == 'away' else home_on).columns:
                (away_on if side == 'away' else home_on)[col] = '\xa0'

    game = pd.concat([merged, home_on, away_on], axis = 1)

    game = game.assign(
    event_team = np.where(game.event_team==game.home_team, game.home_team_abbreviated,
                         np.where(game.event_team==game.away_team, game.away_team_abbreviated,
                                 game.event_team)),
    description = game.description.astype(str))

    game['description'] = np.where(game.description=='nan', '\xa0', game.description)

    game = game.drop(columns = ['original_index', 'strength', 'original_time', 'home_team', 'away_team', 'other_team', 'event_player_str',
                                'version', 'team', 'change_before_event', 'prio', 'change_prio', 'priority', 'tmp', 'tmp2']).rename(
        columns = {'away_team_abbreviated':'away_team', 'home_team_abbreviated':'home_team', 'coordsx':'coords_x', 'coordsy':'coords_y',
                    'ep1_name':'event_player_1', 'ep2_name':'event_player_2', 'ep3_name':'event_player_3'})

    # OPTIMIZED: Pre-compile regex and use vectorized operations where possible
    # event_zone: combine the two apply() calls into one
    def extract_zone(desc):
        match = _ZONE_PATTERN.search(str(desc))
        return match.group() if match else None
    
    # OPTIMIZED: event_detail - reduce string operations by caching splits
    def extract_detail(row):
        desc = row['description']
        event = row['event']
        if pd.isna(desc):
            return '\xa0'
        if event in ['SHOT', 'BLOCK', 'MISS', 'GOAL']:
            parts = desc.split(', ')
            return parts[1].strip() if len(parts) > 1 else '\xa0'
        elif event in ["PSTR", "PEND", "SOC", "GEND"]:
            parts = desc.split(': ')
            return parts[1].strip() if len(parts) > 1 else '\xa0'
        elif event == 'PENL':
            match = _PARENTHESIS_PATTERN.search(desc)
            return match.group(1).strip() if match else '\xa0'
        elif event == 'CHANGE':
            parts = desc.split(' - ')
            return parts[0].strip() if len(parts) > 0 else '\xa0'
        return '\xa0'
    
    game = game.assign(
        game_id = int(game_id),
        season = int(season),
        event_zone = game.description.apply(extract_zone),
        event_detail = game.apply(extract_detail, axis=1))

    # Goalie finding - keep nested np.where() as it's actually quite fast for this use case
    game = game.assign(home_goalie = np.where(
    game.home_on_1.isin(goalies.Name), game.home_on_1,
    np.where(
    game.home_on_2.isin(goalies.Name), game.home_on_2,
    np.where(
    game.home_on_3.isin(goalies.Name), game.home_on_3,
    np.where(
    game.home_on_4.isin(goalies.Name), game.home_on_4,
    np.where(
    game.home_on_5.isin(goalies.Name), game.home_on_5,
    np.where(
    game.home_on_6.isin(goalies.Name), game.home_on_6,
    np.where(
    game.home_on_7.isin(goalies.Name), game.home_on_7,
    np.where(
    game.home_on_8.isin(goalies.Name), game.home_on_8,
    np.where(
    game.home_on_9.isin(goalies.Name), game.home_on_9,
    '\xa0'))))))))),
    away_goalie = np.where(
    game.away_on_1.isin(goalies.Name), game.away_on_1,
    np.where(
    game.away_on_2.isin(goalies.Name), game.away_on_2,
    np.where(
    game.away_on_3.isin(goalies.Name), game.away_on_3,
    np.where(
    game.away_on_4.isin(goalies.Name), game.away_on_4,
    np.where(
    game.away_on_5.isin(goalies.Name), game.away_on_5,
    np.where(
    game.away_on_6.isin(goalies.Name), game.away_on_6,
    np.where(
    game.away_on_7.isin(goalies.Name), game.away_on_7,
    np.where(
    game.away_on_8.isin(goalies.Name), game.away_on_8,
    np.where(
    game.away_on_9.isin(goalies.Name), game.away_on_9,
    '\xa0'))))))))))

    # OPTIMIZED: Vectorized column cleaning - use np.where() in loop for consistency
    # Slightly faster than individual assigns due to reduced function call overhead
    on_ice_cols = [f'{side}_on_{i}' for side in ['away', 'home'] for i in range(1, 10)]
    goalie_cols = ['home_goalie', 'away_goalie']
    
    for col in on_ice_cols + goalie_cols:
        if col in game.columns:
            game[col] = np.where((pd.isna(game[col])) | (game[col] == '') | (game[col] == '\xa0'), '\xa0', game[col])

    # OPTIMIZED: Vectorized skater counting using .ne() and .sum()
    # Cache the game_id check to avoid repeated string operations
    game_id_str = str(game_id)
    is_playoff = int(game_id_str[5]) == 3 if len(game_id_str) > 5 else False
    
    # Vectorized: use .ne() (not equal) which is faster than np.where() for boolean conversion
    home_on_cols = [f'home_on_{i}' for i in range(1, 10)]
    away_on_cols = [f'away_on_{i}' for i in range(1, 10)]
    
    home_skaters = game[home_on_cols].ne('\xa0').sum(axis=1)
    away_skaters = game[away_on_cols].ne('\xa0').sum(axis=1)
    
    # Subtract goalie if present and in regulation/playoff
    goalie_mask = ((game.period < 5) | is_playoff)
    home_skaters = home_skaters - ((game.home_goalie != '\xa0') & goalie_mask).astype(int)
    away_skaters = away_skaters - ((game.away_goalie != '\xa0') & goalie_mask).astype(int)
    
    game = game.assign(home_skaters=home_skaters, away_skaters=away_skaters)

    game = game.assign(home_skater_temp = 
                np.where((game.home_goalie=='\xa0') , 'E', game.home_skaters),
           away_skater_temp = 
                np.where((game.away_goalie=='\xa0') , 'E', game.away_skaters))

    # OPTIMIZED: Reuse cached is_playoff from earlier
    game = game.assign(game_strength_state = (game.home_skater_temp.astype(str)) + 'v' + (game.away_skater_temp.astype(str)),
                      event_zone = np.where(game.event_zone is not None, game.event_zone.str.replace(". Zone", "", regex=False), ''),
                      home_score = np.cumsum(np.where((game.event.shift()=='GOAL') & (((game.period<5) | is_playoff)) & (game.event_team.shift()==game.home_team), 1, 0)),
                      away_score = np.cumsum(np.where((game.event.shift()=='GOAL') & (((game.period<5) | is_playoff)) & (game.event_team.shift()==game.away_team), 1, 0))).drop(
        columns = ['home_skater_temp', 'away_skater_temp'])

    game = game.assign(game_score_state = (game.home_score.astype(str)) + 'v' + (game.away_score.astype(str)),
                      game_date = pd.to_datetime(game.game_date[~pd.isna(game.game_date)].iloc[0])
                      )

    game.number_off = np.where((game.jumping_on!='\xa0') & (game.jumping_off=='\xa0'), 0, game.number_off)
    game.number_on = np.where((game.jumping_off!='\xa0') & (game.jumping_on=='\xa0'), 0, game.number_on)

    so = game[game.period==5]

    if len(so)>0 and int(game.game_id.astype(str).str[5].iloc[0]) != 3:
        game = game[game.period<5]
        home = roster[roster.team=='home'].rename(columns = {'teamnum':'home_on_ice', 'Name':'home_goalie_name'}).loc[:, ['home_goalie_name', 'home_on_ice']]
        away = roster[roster.team=='away'].rename(columns = {'teamnum':'away_on_ice', 'Name':'away_goalie_name'}).loc[:, ['away_goalie_name', 'away_on_ice']]
        so = so.merge(away, how = 'left', indicator = True).drop(columns = ['_merge']).merge(home, how = 'left')
        so = so.assign(
        home_goalie = so.home_goalie_name,
        away_goalie = so.away_goalie_name).drop(columns = ['away_goalie_name', 'home_goalie_name'])
        so_winner = so[so.event=='GOAL'].groupby('event_team')['event', 'home_team'].count().reset_index().sort_values(by = ['event', 'event_team'],ascending = False).event_team.iloc[0]
        so = so.assign(
            home_on_1 = so.home_goalie,
            away_on_1 = so.away_goalie,
            home_on_2 = np.where(so.event_team==so.home_team, so.event_player_1, '\xa0'),
            away_on_2 = np.where(so.event_team==so.away_team, so.event_player_1, '\xa0'))
        if len(so[so.event=='PEND'])>0:
            end_event = so[so.event=='PEND'].index.astype(int)[0]
            so = so.assign(
            home_score = np.where((so.index>=end_event) & (so_winner == so.home_team), 1+so.home_score, so.home_score),
            away_score = np.where((so.index>=end_event) & (so_winner == so.away_team), 1+so.away_score, so.away_score))
        game = pd.concat([game, so])

    game['event_length'] = game.game_seconds.shift(-1) - game.game_seconds
    game['event_length'] = (np.where((pd.isna(game.event_length)) | (game.event_length<0), 0, game.event_length)).astype(int)
    game['event_index'] = game.event_index + 1

    # Change MISS events with miss_reason='short' to SHORT
    if 'miss_reason' in game.columns:
        game['event'] = np.where(
            (game['event'] == 'MISS') & (game['miss_reason'].str.lower() == 'short'),
            'SHORT',
            game['event']
        )

    # Change BLOCK events with "BLOCKED BY TEAMMATE" to TEAMMATE_BLOCK
    game['event'] = np.where(
        (game['event'] == 'BLOCK') & (game['description'].str.upper().str.contains('BLOCKED BY TEAMMATE', na=False)),
        'TEAMMATE_BLOCK',
        game['event']
    )

    if 'coords_x' and 'coords_y' in game.columns:
    
        columns = ['season', 'game_id', 'game_date', 'event_index',
        'period', 'game_seconds', 'event', 'description',
        'event_detail', 'event_zone', 'event_team', 'event_player_1',
        'event_player_2', 'event_player_3', 'event_length', 'coords_x',
        'coords_y', 'number_on', 'number_off', 'jumping_on', 'jumping_off',
        'home_on_1', 'home_on_2', 'home_on_3', 'home_on_4', 'home_on_5',
        'home_on_6', 'home_on_7', 'home_on_8', 'home_on_9', 'away_on_1', 'away_on_2', 'away_on_3',
        'away_on_4', 'away_on_5', 'away_on_6', 'away_on_7', 'away_on_8', 'away_on_9', 'home_goalie',
        'away_goalie', 'home_team', 'away_team', 'home_skaters', 'away_skaters',
        'home_score', 'away_score', 'game_score_state', 'game_strength_state', 'coordinate_source', 'miss_reason']

    else:
        
        columns = ['season', 'game_id', 'game_date', 'event_index',
        'period', 'game_seconds', 'event', 'description',
        'event_detail', 'event_zone', 'event_team', 'event_player_1',
        'event_player_2', 'event_player_3', 'event_length', 
        'number_on', 'number_off', 'jumping_on', 'jumping_off',
        'home_on_1', 'home_on_2', 'home_on_3', 'home_on_4', 'home_on_5',
        'home_on_6', 'home_on_7', 'home_on_8', 'home_on_9', 'away_on_1', 'away_on_2', 'away_on_3',
        'away_on_4', 'away_on_5', 'away_on_6', 'away_on_7', 'away_on_8', 'away_on_9', 'home_goalie',
        'away_goalie', 'home_team', 'away_team', 'home_skaters', 'away_skaters',
        'home_score', 'away_score', 'game_score_state', 'game_strength_state']

    game = game.loc[:, columns].rename(
    columns = {'period':'game_period', 'event':'event_type', 'description':'event_description', 'number_on':'num_on', 'number_off':'num_off',
              'jumping_on':'players_on', 'jumping_off':'players_off'}
    )

    if live == True:
        # Compare finalized to events to see if we have events where the PBP does not match the skaters on ice in the shifts.
        # This can happen when players are taking their first shift of the game and thus do not appear in the time on ice HTML page.
        # Find events in finalized where skater count doesn't match, then get rid of everything beneath them.
        comparison_df = events.assign(events_away_skater_count = events.away_skaters.str.count('\xa0'), events_home_skater_count = events.home_skaters.str.count('\xa0'))[
            events.event.isin(ewc)
        ].loc[:,
        ['period', 'game_seconds', 'event', 'description', 'events_away_skater_count', 'events_home_skater_count']].merge(
            game[game.event_type.isin(ewc)].loc[:, ['game_period', 'game_seconds', 'event_index', 'event_type', 'event_description', 'home_skaters', 'away_skaters']].rename(
            columns = {'game_period':'period', 'event_type':'event', 'event_description':'description', 'away_skaters':'pbp_away_skater_count', 'home_skaters':'pbp_home_skater_count'}
        ))

        mismatches = comparison_df[(comparison_df.pbp_away_skater_count != comparison_df.events_away_skater_count) | 
                (comparison_df.pbp_home_skater_count != comparison_df.events_home_skater_count)]

        # We initially just ditched the mismatched event and everything that came beneath it.
        # But then we ran into an error: We can have a play where the player comes on the ice as a penalty expires. We have a "5v4" goal but PBP events show it as 5v5.
        # NHL dot com video description calls it "Power Play Goal" but doesn't show PP under it. 
        # This is pretty tricky to handle. Because the initial "mismatch comparison" was designed to catch events where the shift was just a few seconds off, let's do it this way.

        # Ditch the mismatched event and everything that comes after it!
        # If we have multiple mismatches and not many events after it. 
        if len(mismatches) > 1 and len(game[game.game_seconds >= mismatches.game_seconds.min()]) < 20:
            game = game[game.event_index < mismatches.event_index.min()]

        game[(game.event_type.isin(ewc)) & (game.home_skaters < 0)]

    if live == True:
        mismatches = game[(game.event_type.isin(ewc + ['FAC'])) & ((game.home_skaters < 3) | (game.home_skaters > 6) | (game.away_skaters < 3) | (game.away_skaters > 6))]
        if len(mismatches) > 0:
            game = game[game.event_index < mismatches.event_index.min()]

    return(game)

def fix_missing(single, event_coords, events):
    """
    Fallback merge for events that failed name-based merge.

    Two-tier approach:
    1. Timing-based: Match by (game_seconds, period, event, version) when only ONE
       event of that type happened at that exact moment
    2. ID-based: When multiple events match by timing, use player_id from API events
       matched via portrait_links lookup to disambiguate
    """
    fenwick_events = ('SHOT', 'MISS', 'GOAL', 'BLOCK', 'HIT', 'GIVE', 'TAKE')

    # Find events with missing coordinates that should have them
    missing_mask = (
        events['coords_x'].isna() &
        events['event'].isin(fenwick_events)
    )

    if not missing_mask.any():
        return events

    problems = events[missing_mask].copy()
    recovered_count = 0

    # === TIER 1: Timing-based fallback ===
    # For each problem event, check if there's exactly ONE API event with same timing
    timing_cols = ['game_seconds', 'period', 'event', 'version']

    # Count API events at each timing point
    api_timing_counts = event_coords.groupby(timing_cols).size().reset_index(name='api_count')

    # Find problem events where API has exactly one match at that timing
    problems_with_counts = problems.merge(
        api_timing_counts,
        on=timing_cols,
        how='left'
    )

    # Get API coords for single-match cases
    single_match_mask = problems_with_counts['api_count'] == 1
    if single_match_mask.any():
        single_match_problems = problems_with_counts[single_match_mask]

        # Determine which columns are available in event_coords
        api_cols = ['game_seconds', 'period', 'event', 'version', 'coords_x', 'coords_y']
        if 'player_id' in event_coords.columns:
            api_cols.append('player_id')

        # Merge to get coords (ignoring player name)
        api_coords_subset = event_coords[api_cols].copy()
        api_coords_subset = api_coords_subset.rename(columns={
            'coords_x': 'recovered_x',
            'coords_y': 'recovered_y'
        })

        recovered = single_match_problems.merge(
            api_coords_subset,
            on=timing_cols,
            how='left'
        )

        # Update events DataFrame with recovered coordinates
        for _, row in recovered[recovered['recovered_x'].notna()].iterrows():
            orig_idx = row['event_index']  # Use event_index to find original row
            mask = events['event_index'] == orig_idx
            events.loc[mask, 'coords_x'] = row['recovered_x']
            events.loc[mask, 'coords_y'] = row['recovered_y']
            recovered_count += 1

    # === TIER 2: ID-based fallback for remaining problems ===
    # Check if we still have missing coords
    still_missing_mask = (
        events['coords_x'].isna() &
        events['event'].isin(fenwick_events)
    )

    if still_missing_mask.any() and 'player_id' in event_coords.columns:
        # Load portrait_links for name→id lookup
        try:
            portrait_links = _get_portrait_links_dict()
        except Exception:
            portrait_links = None

        if portrait_links:
            remaining_problems = events[still_missing_mask].copy()

            # Look up player_id for HTML event names
            remaining_problems['html_player_id'] = remaining_problems['event_player_1'].apply(
                lambda name: portrait_links.get(str(name).upper()) if pd.notna(name) else None
            )

            has_id_mask = remaining_problems['html_player_id'].notna()
            if has_id_mask.any():
                problems_with_id = remaining_problems[has_id_mask]

                # Merge on player_id + timing
                api_for_id_merge = event_coords[['player_id', 'game_seconds', 'period',
                                                  'event', 'version', 'coords_x', 'coords_y']].copy()
                api_for_id_merge = api_for_id_merge.rename(columns={
                    'coords_x': 'recovered_x',
                    'coords_y': 'recovered_y'
                })

                id_recovered = problems_with_id.merge(
                    api_for_id_merge,
                    left_on=['html_player_id', 'game_seconds', 'period', 'event', 'version'],
                    right_on=['player_id', 'game_seconds', 'period', 'event', 'version'],
                    how='left'
                )

                for _, row in id_recovered[id_recovered['recovered_x'].notna()].iterrows():
                    orig_idx = row['event_index']
                    mask = events['event_index'] == orig_idx
                    events.loc[mask, 'coords_x'] = row['recovered_x']
                    events.loc[mask, 'coords_y'] = row['recovered_y']
                    recovered_count += 1

    if recovered_count > 0:
        print(f'Fallback merge recovered coordinates for {recovered_count} events')

    return events

def _fetch_all_pages_parallel(season, game_id, verbose=False):
    """
    Fetch all required HTML pages in parallel.
    
    Args:
        season: Season string (e.g., '20242025')
        game_id: Full game ID (e.g., 2025020333)
        verbose: If True, print detailed timing information
    
    Returns:
        Dictionary with keys: 'events', 'roster', 'home_shifts', 'away_shifts'
        All values are requests.Response objects
    """
    small_id = str(game_id)[5:]
    
    # Prepare all URLs
    events_url = f'http://www.nhl.com/scores/htmlreports/{season}/PL0{small_id}.HTM'
    roster_url = f'http://www.nhl.com/scores/htmlreports/{season}/RO0{small_id}.HTM'
    home_shifts_url = f'http://www.nhl.com/scores/htmlreports/{season}/TH0{small_id}.HTM'
    away_shifts_url = f'http://www.nhl.com/scores/htmlreports/{season}/TV0{small_id}.HTM'
    summary_url = f'https://www.nhl.com/scores/htmlreports/{season}/GS0{small_id}.HTM'
    
    # Fetch HTML pages concurrently (4 pages)
    fetch_start = time.time()
    if verbose:
        print('  🔄 Fetching HTML pages in parallel...')
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit HTML fetch tasks only
        futures = {
            'events': executor.submit(_fetch_url, events_url, timeout=10),
            'roster': executor.submit(_fetch_url, roster_url, timeout=10),
            'home_shifts': executor.submit(_fetch_url, home_shifts_url, timeout=10),
            'away_shifts': executor.submit(_fetch_url, away_shifts_url, timeout=10),
            'summary': executor.submit(_fetch_url, summary_url, timeout=10)
        }
        
        # Create reverse mapping from future to key
        future_to_key = {future: key for key, future in futures.items()}
        
        # Collect HTML page results as they complete
        results = {}
        for future in as_completed(futures.values()):
            key = future_to_key[future]
            results[key] = future.result()  # Will raise if HTTP error
    
    html_fetch_duration = time.time() - fetch_start
    if verbose:
        try:
            print(f'  ⏱️ HTML pages fetched in: {html_fetch_duration:.2f}s')
        except Exception:
            pass
    
    return results

def full_scrape_1by1(game_id_list, live = False, shift_to_espn = True, return_intermediates = False, verbose = False):
    
    global single
    global event_coords
    global full
    global fixed_events
    global events
    
    # OPTIMIZED: Use list instead of DataFrame for accumulating results
    full_list = []
    
    # Track intermediates for each game if requested
    intermediates_list = []

    i = 0
    retry_count = 0  # Track retries for transient errors (empty HTML, network issues)
    MAX_RETRIES = 3

    while i in range(0, len(game_id_list)) and len(game_id_list)>0:

        # First thing to try: Scraping HTML events

        try:
            first_time = time.time()
            game_id = game_id_list[i]
            print('Attempting scrape for: ' + str(game_id))
            season = str(int(str(game_id)[:4])) + str(int(str(game_id)[:4]) + 1)
            small_id = str(game_id)[5:]
            
            # OPTIMIZED: Fetch HTML pages in parallel, API separately
            parallel_start = time.time()
            if verbose:
                print('Fetching pages')
            pages = _fetch_all_pages_parallel(season, game_id, verbose=verbose)
            parallel_duration = time.time() - parallel_start
            if verbose:
                try:
                    print(f'⏱️ Parallel fetch took: {parallel_duration:.2f}s')
                except Exception:
                    pass
            
            # TIME: HTML Events (using pre-fetched pages)
            html_start = time.time()
            if verbose:
                print('Scraping HTML events')
            single, roster_cache = scrape_html_events(season, small_id, 
                                                      events_page=pages['events'], 
                                                      roster_page=pages['roster'],
                                                      verbose=verbose)
            html_duration = time.time() - html_start
            if verbose:
                try:
                    print(f'⏱️ HTML events processing took: {html_duration:.2f}s')
                except Exception:
                    pass
            single['game_id'] = int(game_id)
            
            # Try NHL API first (default behavior)
            
            try:
                # TIME: API Events (fetch after HTML events are processed, like original)
                api_start = time.time()
                if verbose:
                    print('Attempting to scrape coordinates from NHL API')
                event_coords = scrape_api_events(game_id, drop_description=True, verbose=verbose)
                api_duration = time.time() - api_start
                if verbose:
                    try:
                        print(f'⏱️ API events took: {api_duration:.2f}s')
                    except Exception:
                        pass
                
                # Set coordinate_source on event_coords before merging (needed for fix_missing)
                event_coords['coordinate_source'] = 'api'
                api_coords = event_coords.copy()
                if len(event_coords[(event_coords.event.isin(ewc)) & (pd.isna(event_coords.coords_x))]) > 0:
                    raise ExpatError('Bad takes, dude!')
                event_coords['game_id'] = int(game_id)
                
                # TIME: Merge Events
                merge_start = time.time()
                if verbose:
                    print('Attempting to merge events')
                events = single.merge(event_coords, on = ['event_player_1', 'game_seconds', 'version', 'period', 'game_id', 'event'], how = 'left')
                merge_duration = time.time() - merge_start
                if verbose:
                    print(f'Merged events, we have this many rows: {len(events)}')
                    try:
                        print(f'⏱️ Merge took: {merge_duration:.2f}s')
                    except Exception:
                        pass
                

                
                # TIME: Fix Missing
                try:
                    fix_start = time.time()
                    events = fix_missing(single, event_coords, events)
                    fix_duration = time.time() - fix_start
                    if verbose:
                        try:
                            print(f'⏱️ Fix missing took: {fix_duration:.2f}s')
                        except Exception:
                            pass
                except IndexError as e:
                    print('Issue when fixing problematic events. Here it is: ' + str(e))
                    continue
                if verbose:
                    print(pages)
                # TIME: Shifts and Finalize (using pre-fetched pages)
                try:
                    if verbose:
                        print(pages)
                    shifts_start = time.time()
                    if live == True:
                        min_game_clock, shifts = scrape_html_shifts(season, small_id, live, 
                                                    home_page=pages['home_shifts'],
                                                    away_page=pages['away_shifts'],
                                                    summary = pages['summary'],
                                                    roster_cache = roster_cache,
                                                    verbose=verbose)
                    else:
                        shifts = scrape_html_shifts(season, small_id, live, 
                                                    home_page=pages['home_shifts'],
                                                    away_page=pages['away_shifts'],
                                                    summary = pages['summary'],
                                                    roster_cache = roster_cache,
                                                    verbose=verbose)
                    shifts_duration = time.time() - shifts_start
                    if verbose:
                        try:
                            print(f'⏱️ HTML shifts processing took: {shifts_duration:.2f}s')
                        except Exception:
                            pass
                    
                    prepare_start = time.time()
                    finalized = merge_and_prepare(events, shifts, roster_cache, live = live)
                    if live == True:
                        if min_game_clock is not None:
                            finalized = finalized[finalized.game_seconds <= min_game_clock]
                    prepare_duration = time.time() - prepare_start
                    if verbose:
                        try:
                            print(f'⏱️ Merge and prepare took: {prepare_duration:.2f}s')
                        except Exception:
                            pass
                    
                    full_list.append(finalized)
                    second_time = time.time()
                    
                    # Track intermediates if requested
                    if return_intermediates:
                        intermediates_list.append({
                            'game_id': game_id,
                            'shifts': shifts.copy() if shifts is not None else None,
                            'api_coords': api_coords.copy() if 'api_coords' in locals() else None,
                            'roster_cache': roster_cache.copy() if roster_cache is not None else None,
                            'coordinate_source': 'api',
                            'warning': None,
                            'error': None,
                            'raw_html': {
                                'events': pages.get('events'),
                                'roster': pages.get('roster'),
                                'home_shifts': pages.get('home_shifts'),
                                'away_shifts': pages.get('away_shifts'),
                                'summary': pages.get('summary')
                            }
                        })
                except IndexError as e:
                    print('There was no shift data for this game. Error: ' + str(e))
                    fixed_events = events
                    fixed_events = fixed_events.rename(
                    columns = {'period':'game_period', 'event':'event_type', 'away_team_abbreviated':'away_team', 
                              'home_team_abbreviated':'home_team', 'description':'event_description', 'home_team':'hometeamfull',
                              'away_team':'awayteamfull'}
                    ).drop(
                    columns = ['original_time', 'other_team', 'strength', 'event_player_str', 'version', 'hometeamfull', 'awayteamfull']
                    ).assign(game_warning = 'NO SHIFT DATA.')
                    full_list.append(fixed_events)
                    second_time = time.time()
                    
                    # Track intermediates if requested
                    if return_intermediates:
                        intermediates_list.append({
                            'game_id': game_id,
                            'shifts': None,
                            'api_coords': api_coords.copy() if 'api_coords' in locals() else None,
                            'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                            'coordinate_source': 'api',
                            'warning': 'NO SHIFT DATA.',
                            'error': None,
                            'raw_html': {
                                'events': pages.get('events'),
                                'roster': pages.get('roster'),
                                'home_shifts': pages.get('home_shifts'),
                                'away_shifts': pages.get('away_shifts'),
                                'summary': pages.get('summary')
                            }
                        })
                
                try:
                    total_duration = second_time - first_time
                except NameError:
                    second_time = time.time()
                    total_duration = second_time - first_time
                print('Successfully scraped ' + str(game_id) + '. Coordinates sourced from the NHL API.')
                # Safely format timing string, handling potentially undefined variables
                try:
                    timing_parts = []
                    if 'parallel_duration' in locals(): timing_parts.append(f"Parallel fetch: {parallel_duration:.2f}s")
                    if 'html_duration' in locals(): timing_parts.append(f"HTML processing: {html_duration:.2f}s")
                    if 'api_duration' in locals(): timing_parts.append(f"API processing: {api_duration:.2f}s")
                    if 'merge_duration' in locals(): timing_parts.append(f"Merge: {merge_duration:.2f}s")
                    if 'fix_duration' in locals(): timing_parts.append(f"Fix missing: {fix_duration:.2f}s")
                    if 'shifts_duration' in locals(): timing_parts.append(f"Shifts: {shifts_duration:.2f}s")
                    if 'prepare_duration' in locals(): timing_parts.append(f"Merge/prepare: {prepare_duration:.2f}s")
                    if len(timing_parts) > 0:
                        print("(" + ", ".join(timing_parts) + ")")
                    else:
                        print(f"⏱️ TOTAL game scrape: {total_duration:.2f}s")
                except Exception:
                    print(f"⏱️ TOTAL game scrape: {total_duration:.2f}s")
                i = i + 1
                retry_count = 0  # Reset for next game

                # If there is an issue with the API, fall back to ESPN:
                
            except (KeyError, ExpatError) as e:
                print('The NHL API gave us trouble with: ' + str(game_id) + '. Falling back to ESPN.')
                
                try:
                    home_team = single['home_team_abbreviated'].iloc[0]
                    away_team = single['away_team_abbreviated'].iloc[0]
                    game_date = single['game_date'].iloc[0]
                    espn_home_team = home_team
                    espn_away_team = away_team
                    try:
                        if home_team == 'T.B':
                            espn_home_team = 'TBL'
                        if away_team == 'T.B':
                            espn_away_team = 'TBL'
                        if home_team == 'L.A':
                            espn_home_team = 'LAK'
                        if away_team == 'L.A':
                            espn_away_team = 'LAK'
                        if home_team == 'N.J':
                            espn_home_team = 'NJD'
                        if away_team == 'N.J':
                            espn_away_team = 'NJD'
                        if home_team == 'S.J':
                            espn_home_team = 'SJS'
                        if away_team == 'S.J':
                            espn_away_team = 'SJS'
                        if verbose:
                            print('Scraping ESPN IDs')
                        espn_id = scrape_espn_ids_single_game(str(game_date.date()), espn_home_team, espn_away_team).espn_id.iloc[0]
                        if verbose:
                            print('Scraping ESPN Events')
                            print('Here is the ESPN ID:', espn_id)
                        event_coords = scrape_espn_events(int(espn_id))
                        if verbose:
                            print('Scraped ESPN Events, we have this many rows:', len(event_coords))
                        event_coords['coordinate_source'] = 'espn'
                        if verbose:
                            print('Attempting to merge events')
                        events = single.merge(event_coords, on = ['event_player_1', 'game_seconds', 'period', 'version', 'event'], how = 'left').drop(columns = ['espn_id'])
                        if verbose:
                            print('Merged events, we have this many rows:', len(events))
                        try:
                            events = fix_missing(single, event_coords, events)
                        except IndexError as e:
                            print('Issue when fixing problematic events. Here it is: ' + str(e))
                            continue
                    except IndexError:
                        print('This game does not have ESPN or API coordinates. You will get it anyway, though.')
                        events = single
                    try:
                        shifts = scrape_html_shifts(season, small_id, live,
                                                    home_page=pages['home_shifts'],
                                                    away_page=pages['away_shifts'],
                                                    summary = pages['summary'],
                                                    roster_cache = roster_cache,
                                                    verbose=verbose)
                        finalized = merge_and_prepare(events, shifts, roster_cache, live = live)
                        full_list.append(finalized)
                        second_time = time.time()
                        
                        # Track intermediates if requested
                        if return_intermediates:
                            intermediates_list.append({
                                'game_id': game_id,
                                'shifts': shifts.copy() if shifts is not None else None,
                                'api_coords': None,
                                'roster_cache': roster_cache.copy() if roster_cache is not None else None,
                                'coordinate_source': 'espn',
                                'warning': None,
                                'error': None,
                                'raw_html': {
                                    'events': pages.get('events'),
                                    'roster': pages.get('roster'),
                                    'home_shifts': pages.get('home_shifts'),
                                    'away_shifts': pages.get('away_shifts'),
                                    'summary': pages.get('summary')
                                }
                            })
                    except IndexError as e:
                        print('There was no shift data for this game. Error: ' + str(e))
                        fixed_events = events
                        fixed_events = fixed_events.rename(
                        columns = {'period':'game_period', 'event':'event_type', 'away_team_abbreviated':'away_team', 
                                  'home_team_abbreviated':'home_team', 'description':'event_description', 'home_team':'hometeamfull',
                                  'away_team':'awayteamfull'}
                        ).drop(
                        columns = ['original_time', 'other_team', 'strength', 'event_player_str', 'version', 'hometeamfull', 'awayteamfull']
                        ).assign(game_warning = 'NO SHIFT DATA', season = season)
                        fixed_events['coordinate_source'] = 'espn'
                        full_list.append(fixed_events)
                        
                        # Track intermediates if requested
                        if return_intermediates:
                            intermediates_list.append({
                                'game_id': game_id,
                                'shifts': None,
                                'api_coords': None,
                                'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                                'coordinate_source': 'espn',
                                'warning': 'NO SHIFT DATA',
                                'error': None,
                                'raw_html': {
                                    'events': pages.get('events'),
                                    'roster': pages.get('roster'),
                                    'home_shifts': pages.get('home_shifts'),
                                    'away_shifts': pages.get('away_shifts'),
                                    'summary': pages.get('summary')
                                }
                            })
                    second_time = time.time()
                    total_duration = second_time - first_time
                    # Fix this so it doesn't say sourced from ESPN if no coords.
                    if single.equals(events):
                        if verbose:
                            print("This game took " + str(round(total_duration, 2)) + " seconds.")
                        i = i + 1
                        retry_count = 0  # Reset for next game
                    else:
                        print('Successfully scraped ' + str(game_id) + '. Coordinates sourced from ESPN.')
                        try:
                            timing_parts = []
                            if 'parallel_duration' in locals(): timing_parts.append(f"Parallel fetch: {parallel_duration:.2f}s")
                            if 'html_duration' in locals(): timing_parts.append(f"HTML processing: {html_duration:.2f}s")
                            if len(timing_parts) > 0:
                                print("(" + ", ".join(timing_parts) + ")")
                            else:
                                print(f"⏱️ TOTAL game scrape: {total_duration:.2f}s")
                        except Exception:
                            print(f"⏱️ TOTAL game scrape: {total_duration:.2f}s")
                        i = i + 1
                        retry_count = 0  # Reset for next game

                    # If there are issues with ESPN

                except KeyError as e:
                    print('ESPN also had trouble scraping coordinates for: ' + str(game_id) + '. Looks like we will need to punt this one, unfortunately.')
                    print('KeyError: ' + str(e))
                    print(traceback.format_exc())
                    if return_intermediates:
                        intermediates_list.append({
                            'game_id': game_id,
                            'shifts': None,
                            'api_coords': None,
                            'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                            'coordinate_source': None,
                            'warning': None,
                            'error': f'ESPN KeyError: {str(e)}',
                            'error_traceback': traceback.format_exc(),
                            'raw_html': {
                                'events': pages.get('events') if 'pages' in locals() else None,
                                'roster': pages.get('roster') if 'pages' in locals() else None,
                                'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                                'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                                'summary': pages.get('summary') if 'pages' in locals() else None
                            }
                        })
                    i = i + 1
                    retry_count = 0  # Reset for next game
                    continue
                except IndexError as e:
                    print('ESPN also had trouble scraping coordinates for: ' + str(game_id) + '. Looks like we will need to punt this one, unfortunately.')
                    print('IndexError: ' + str(e))
                    if return_intermediates:
                        intermediates_list.append({
                            'game_id': game_id,
                            'shifts': None,
                            'api_coords': None,
                            'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                            'coordinate_source': None,
                            'warning': None,
                            'error': f'ESPN IndexError: {str(e)}',
                            'error_traceback': traceback.format_exc(),
                            'raw_html': {
                                'events': pages.get('events') if 'pages' in locals() else None,
                                'roster': pages.get('roster') if 'pages' in locals() else None,
                                'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                                'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                                'summary': pages.get('summary') if 'pages' in locals() else None
                            }
                        })
                    i = i + 1
                    retry_count = 0  # Reset for next game
                    continue
                except TypeError as e:
                    print('ESPN also had trouble scraping coordinates for: ' + str(game_id) + '. Looks like we will need to punt this one, unfortunately.')
                    print('TypeError: ' + str(e))
                    if return_intermediates:
                        intermediates_list.append({
                            'game_id': game_id,
                            'shifts': None,
                            'api_coords': None,
                            'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                            'coordinate_source': None,
                            'warning': None,
                            'error': f'ESPN TypeError: {str(e)}',
                            'error_traceback': traceback.format_exc(),
                            'raw_html': {
                                'events': pages.get('events') if 'pages' in locals() else None,
                                'roster': pages.get('roster') if 'pages' in locals() else None,
                                'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                                'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                                'summary': pages.get('summary') if 'pages' in locals() else None
                            }
                        })
                    i = i + 1
                    retry_count = 0  # Reset for next game
                    continue
                except ExpatError as e:
                    print('ESPN also had trouble scraping coordinates for: ' + str(game_id) + '. Looks like we will need to punt this one, unfortunately.')
                    print('ExpatError: ' + str(e))
                    if return_intermediates:
                        intermediates_list.append({
                            'game_id': game_id,
                            'shifts': None,
                            'api_coords': None,
                            'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                            'coordinate_source': None,
                            'warning': None,
                            'error': f'ESPN ExpatError: {str(e)}',
                            'error_traceback': traceback.format_exc(),
                            'raw_html': {
                                'events': pages.get('events') if 'pages' in locals() else None,
                                'roster': pages.get('roster') if 'pages' in locals() else None,
                                'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                                'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                                'summary': pages.get('summary') if 'pages' in locals() else None
                            }
                        })
                    i = i + 1
                    retry_count = 0  # Reset for next game
                    continue

            except ExpatError:
                print('There was a rare error with the API; numerous takeaways did not have location coordinates for: ' + str(game_id) + '. Let us try ESPN.')
                
                try:
                    home_team = single['home_team'].iloc[0]
                    away_team = single['away_team'].iloc[0]
                    game_date = single['game_date'].iloc[0]
                    try:
                        espn_id = scrape_espn_ids_single_game(str(game_date.date()), home_team, away_team).espn_id.iloc[0]
                        event_coords = scrape_espn_events(int(espn_id))
                        duped_coords = api_coords.assign(source = 'api').merge(event_coords.drop(columns = 'espn_id'), on = ['game_seconds', 'event', 'period', 'version', 'event_player_1'], how = 'outer', indicator = True)
                        # Coordinates are flipped in some games.
                        if len(duped_coords[duped_coords.coords_x_x * -1 == duped_coords.coords_x_y])/len(duped_coords):
                            duped_coords['coords_x_y'] = duped_coords['coords_x_y'] * (-1)
                        if len(duped_coords[duped_coords.coords_y_x * -1 == duped_coords.coords_y_y])/len(duped_coords):
                            duped_coords['coords_y_y'] = duped_coords['coords_y_y'] * (-1)
                        duped_coords['source'] = np.where((pd.isna(duped_coords.source)) | ((pd.isna(duped_coords.coords_x_x)) & ~pd.isna(duped_coords.coords_x_y)), 'espn', duped_coords.source)
                        duped_coords = duped_coords.assign(coords_x = np.where(pd.isna(duped_coords.coords_x_x), duped_coords.coords_x_y, duped_coords.coords_x_x),
                                          coords_y = np.where(pd.isna(duped_coords.coords_y_x), duped_coords.coords_y_y, duped_coords.coords_y_x))
                        col_list = list(api_coords.columns)
                        col_list._append('source')
                        duped_coords = duped_coords.loc[:, col_list]
                        duped_coords = duped_coords[duped_coords.event.isin(['SHOT', 'HIT', 'BLOCK', 'MISS', 'GIVE', 'TAKE', 'GOAL', 'PENL', 'FAC'])]
                        duped_coords = duped_coords[~duped_coords.duplicated()]
                        event_coords = duped_coords
                        events = single.merge(event_coords, on = ['event_player_1', 'game_seconds', 'period', 'version', 'event'], how = 'left')#.drop(columns = ['espn_id'])
                        try:
                            events = fix_missing(single, event_coords, events)
                            events['coordinate_source'] = events['source']
                        except IndexError as e:
                            print('Issue when fixing problematic events. Here it is: ' + str(e))
                    except IndexError as e:
                        if event_coords is not None:
                            print('Okay, ESPN had issues. We will go back to the API for this one. Issue: ' + str(e))
                            events = single.merge(event_coords, on = ['event_player_1', 'game_seconds', 'version', 'period', 'event'], how = 'left')
                            try:
                                events = fix_missing(single, event_coords, events)
                            except IndexError as e:
                                print('Issue when fixing problematic events. Here it is: ' + str(e))
                        else:
                            print('This game does not have ESPN or API coordinates. You will get it anyway, though. Issue: ' + str(e))
                            events = single
                            events['coordinate_source'] = 'none'
                    # Determine coordinate source for this path
                    coord_source_for_intermediates = 'espn'
                    if 'source' in events.columns:
                        coord_source_for_intermediates = 'api_espn_hybrid'
                    elif 'coordinate_source' not in events.columns or events.get('coordinate_source', pd.Series(['none'])).iloc[0] == 'none':
                        coord_source_for_intermediates = 'none'
                    
                    try:
                        shifts = scrape_html_shifts(season, small_id, live,
                                                    home_page=pages['home_shifts'],
                                                    away_page=pages['away_shifts'],
                                                    summary = pages['summary'],
                                                    roster_cache = roster_cache,
                                                    verbose=verbose)
                        finalized = merge_and_prepare(events, shifts, roster_cache, live = live)
                        full_list.append(finalized)
                        second_time = time.time()
                        
                        # Track intermediates if requested
                        if return_intermediates:
                            intermediates_list.append({
                                'game_id': game_id,
                                'shifts': shifts.copy() if shifts is not None else None,
                                'api_coords': api_coords.copy() if 'api_coords' in locals() else None,
                                'roster_cache': roster_cache.copy() if roster_cache is not None else None,
                                'coordinate_source': coord_source_for_intermediates,
                                'warning': None,
                                'error': None,
                                'raw_html': {
                                    'events': pages.get('events'),
                                    'roster': pages.get('roster'),
                                    'home_shifts': pages.get('home_shifts'),
                                    'away_shifts': pages.get('away_shifts'),
                                    'summary': pages.get('summary')
                                }
                            })
                    except IndexError as e:
                        print('There was no shift data for this game. Error: ' + str(e))
                        fixed_events = events
                        fixed_events = fixed_events.rename(
                        columns = {'period':'game_period', 'event':'event_type', 'away_team_abbreviated':'away_team', 
                                  'home_team_abbreviated':'home_team', 'description':'event_description', 'home_team':'hometeamfull',
                                  'away_team':'awayteamfull'}
                        ).drop(
                        columns = ['original_time', 'other_team', 'strength', 'event_player_str', 'version', 'hometeamfull', 'awayteamfull']
                        ).assign(game_warning = 'NO SHIFT DATA', season = season)
                        full_list.append(fixed_events)
                        
                        # Track intermediates if requested
                        if return_intermediates:
                            intermediates_list.append({
                                'game_id': game_id,
                                'shifts': None,
                                'api_coords': api_coords.copy() if 'api_coords' in locals() else None,
                                'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                                'coordinate_source': coord_source_for_intermediates,
                                'warning': 'NO SHIFT DATA',
                                'error': None,
                                'raw_html': {
                                    'events': pages.get('events'),
                                    'roster': pages.get('roster'),
                                    'home_shifts': pages.get('home_shifts'),
                                    'away_shifts': pages.get('away_shifts'),
                                    'summary': pages.get('summary')
                                }
                            })
                    second_time = time.time()
                    total_duration = second_time - first_time
                    # Fix this so it doesn't say sourced from ESPN if no coords.
                    print('Successfully scraped ' + str(game_id) + '. Coordinates sourced from ESPN.')
                    try:
                        timing_parts = []
                        if 'parallel_duration' in locals(): timing_parts.append(f"Parallel fetch: {parallel_duration:.2f}s")
                        if 'html_duration' in locals(): timing_parts.append(f"HTML processing: {html_duration:.2f}s")
                        if len(timing_parts) > 0:
                            print("(" + ", ".join(timing_parts) + ")")
                        else:
                            print(f"⏱️ TOTAL game scrape: {total_duration:.2f}s")
                    except Exception:
                        print(f"⏱️ TOTAL game scrape: {total_duration:.2f}s")
                    i = i + 1
                    retry_count = 0  # Reset for next game

                    # If there are issues with ESPN

                except KeyError as e:
                    print('ESPN also had trouble scraping coordinates for: ' + str(game_id) + '. Looks like we will need to punt this one, unfortunately.')
                    print('KeyError: ' + str(e))
                    if return_intermediates:
                        intermediates_list.append({
                            'game_id': game_id,
                            'shifts': None,
                            'api_coords': api_coords.copy() if 'api_coords' in locals() else None,
                            'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                            'coordinate_source': None,
                            'warning': None,
                            'error': f'ESPN Hybrid KeyError: {str(e)}',
                            'error_traceback': traceback.format_exc(),
                            'raw_html': {
                                'events': pages.get('events') if 'pages' in locals() else None,
                                'roster': pages.get('roster') if 'pages' in locals() else None,
                                'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                                'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                                'summary': pages.get('summary') if 'pages' in locals() else None
                            }
                        })
                    i = i + 1
                    retry_count = 0  # Reset for next game
                    continue
                except IndexError as e:
                    print('ESPN also had trouble scraping coordinates for: ' + str(game_id) + '. Looks like we will need to punt this one, unfortunately.')
                    print('IndexError: ' + str(e))
                    if return_intermediates:
                        intermediates_list.append({
                            'game_id': game_id,
                            'shifts': None,
                            'api_coords': api_coords.copy() if 'api_coords' in locals() else None,
                            'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                            'coordinate_source': None,
                            'warning': None,
                            'error': f'ESPN Hybrid IndexError: {str(e)}',
                            'error_traceback': traceback.format_exc(),
                            'raw_html': {
                                'events': pages.get('events') if 'pages' in locals() else None,
                                'roster': pages.get('roster') if 'pages' in locals() else None,
                                'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                                'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                                'summary': pages.get('summary') if 'pages' in locals() else None
                            }
                        })
                    i = i + 1
                    retry_count = 0  # Reset for next game
                    continue
                except TypeError as e:
                    print('ESPN also had trouble scraping coordinates for: ' + str(game_id) + '. Looks like we will need to punt this one, unfortunately.')
                    print('TypeError: ' + str(e))
                    if return_intermediates:
                        intermediates_list.append({
                            'game_id': game_id,
                            'shifts': None,
                            'api_coords': api_coords.copy() if 'api_coords' in locals() else None,
                            'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                            'coordinate_source': None,
                            'warning': None,
                            'error': f'ESPN Hybrid TypeError: {str(e)}',
                            'error_traceback': traceback.format_exc(),
                            'raw_html': {
                                'events': pages.get('events') if 'pages' in locals() else None,
                                'roster': pages.get('roster') if 'pages' in locals() else None,
                                'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                                'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                                'summary': pages.get('summary') if 'pages' in locals() else None
                            }
                        })
                    i = i + 1
                    retry_count = 0  # Reset for next game
                    continue
                except ExpatError as e:
                    print('ESPN also had trouble scraping coordinates for: ' + str(game_id) + '. Looks like we will need to punt this one, unfortunately.')
                    print('ExpatError: ' + str(e))
                    if return_intermediates:
                        intermediates_list.append({
                            'game_id': game_id,
                            'shifts': None,
                            'api_coords': api_coords.copy() if 'api_coords' in locals() else None,
                            'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                            'coordinate_source': None,
                            'warning': None,
                            'error': f'ESPN Hybrid ExpatError: {str(e)}',
                            'error_traceback': traceback.format_exc(),
                            'raw_html': {
                                'events': pages.get('events') if 'pages' in locals() else None,
                                'roster': pages.get('roster') if 'pages' in locals() else None,
                                'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                                'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                                'summary': pages.get('summary') if 'pages' in locals() else None
                            }
                        })
                    i = i + 1
                    retry_count = 0  # Reset for next game
                    continue

        except ConnectionError as e:
            print('Got a Connection Error, time to sleep.')
            if return_intermediates:
                intermediates_list.append({
                    'game_id': game_id if 'game_id' in locals() else game_id_list[i],
                    'shifts': None,
                    'api_coords': None,
                    'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                    'coordinate_source': None,
                    'warning': None,
                    'error': f'ConnectionError: {str(e)}',
                    'error_traceback': traceback.format_exc(),
                    'raw_html': {
                        'events': pages.get('events') if 'pages' in locals() else None,
                        'roster': pages.get('roster') if 'pages' in locals() else None,
                        'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                        'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                        'summary': pages.get('summary') if 'pages' in locals() else None
                    }
                })
            time.sleep(10)
            continue
            
        except ChunkedEncodingError as e:
            print('Got a Connection Error, time to sleep.')
            if return_intermediates:
                intermediates_list.append({
                    'game_id': game_id if 'game_id' in locals() else game_id_list[i],
                    'shifts': None,
                    'api_coords': None,
                    'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                    'coordinate_source': None,
                    'warning': None,
                    'error': f'ChunkedEncodingError: {str(e)}',
                    'error_traceback': traceback.format_exc(),
                    'raw_html': {
                        'events': pages.get('events') if 'pages' in locals() else None,
                        'roster': pages.get('roster') if 'pages' in locals() else None,
                        'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                        'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                        'summary': pages.get('summary') if 'pages' in locals() else None
                    }
                })
            time.sleep(10)
            continue
            
        except AttributeError as e:
            print(str(game_id) + ' does not have an HTML report. Here is the error: ' + str(e))
            print(traceback.format_exc())
            if return_intermediates:
                intermediates_list.append({
                    'game_id': game_id if 'game_id' in locals() else game_id_list[i],
                    'shifts': None,
                    'api_coords': None,
                    'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                    'coordinate_source': None,
                    'warning': None,
                    'error': f'AttributeError: {str(e)}',
                    'error_traceback': traceback.format_exc(),
                    'raw_html': {
                        'events': pages.get('events') if 'pages' in locals() else None,
                        'roster': pages.get('roster') if 'pages' in locals() else None,
                        'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                        'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                        'summary': pages.get('summary') if 'pages' in locals() else None
                    }
                })
            # Retry transient HTML errors with backoff
            if retry_count < MAX_RETRIES:
                retry_count += 1
                delay = 5 * retry_count  # 5, 10, 15 seconds
                print(f"  Retrying {game_id} in {delay} seconds... (attempt {retry_count}/{MAX_RETRIES})")
                time.sleep(delay)
                continue  # Retry same game
            else:
                print(f"  All {MAX_RETRIES} retries exhausted for {game_id}, skipping.")
                retry_count = 0  # Reset for next game
                i = i + 1
                continue
            
        except IndexError as e:
            print(str(game_id) + ' has an issue with the HTML Report. Here is the error: ' + str(e))
            print(traceback.format_exc())
            if return_intermediates:
                intermediates_list.append({
                    'game_id': game_id if 'game_id' in locals() else game_id_list[i],
                    'shifts': shifts.copy() if 'shifts' in locals() and shifts is not None else None,
                    'api_coords': api_coords.copy() if 'api_coords' in locals() else None,
                    'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                    'coordinate_source': None,
                    'warning': None,
                    'error': f'IndexError: {str(e)}',
                    'error_traceback': traceback.format_exc(),
                    'raw_html': {
                        'events': pages.get('events') if 'pages' in locals() else None,
                        'roster': pages.get('roster') if 'pages' in locals() else None,
                        'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                        'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                        'summary': pages.get('summary') if 'pages' in locals() else None
                    }
                })
            i = i + 1
            retry_count = 0  # Reset for next game
            continue

        except ValueError as e:
            print(str(game_id) + ' has an issue with the HTML Report. Here is the error: ' + str(e))
            print(traceback.format_exc())
            if return_intermediates:
                intermediates_list.append({
                    'game_id': game_id if 'game_id' in locals() else game_id_list[i],
                    'shifts': shifts.copy() if 'shifts' in locals() and shifts is not None else None,
                    'api_coords': api_coords.copy() if 'api_coords' in locals() else None,
                    'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                    'coordinate_source': None,
                    'warning': None,
                    'error': f'ValueError: {str(e)}',
                    'error_traceback': traceback.format_exc(),
                    'raw_html': {
                        'events': pages.get('events') if 'pages' in locals() else None,
                        'roster': pages.get('roster') if 'pages' in locals() else None,
                        'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                        'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                        'summary': pages.get('summary') if 'pages' in locals() else None
                    }
                })
            # Retry transient HTML errors with backoff
            if retry_count < MAX_RETRIES:
                retry_count += 1
                delay = 5 * retry_count  # 5, 10, 15 seconds
                print(f"  Retrying {game_id} in {delay} seconds... (attempt {retry_count}/{MAX_RETRIES})")
                time.sleep(delay)
                continue  # Retry same game
            else:
                print(f"  All {MAX_RETRIES} retries exhausted for {game_id}, skipping.")
                retry_count = 0  # Reset for next game
                i = i + 1
                continue

        except KeyError as k:
            print(str(game_id) + 'gave some kind of Key Error. Here is the error: ' + str(k))
            if return_intermediates:
                intermediates_list.append({
                    'game_id': game_id if 'game_id' in locals() else game_id_list[i],
                    'shifts': shifts.copy() if 'shifts' in locals() and shifts is not None else None,
                    'api_coords': api_coords.copy() if 'api_coords' in locals() else None,
                    'roster_cache': roster_cache.copy() if 'roster_cache' in locals() and roster_cache is not None else None,
                    'coordinate_source': None,
                    'warning': None,
                    'error': f'KeyError: {str(k)}',
                    'error_traceback': traceback.format_exc(),
                    'raw_html': {
                        'events': pages.get('events') if 'pages' in locals() else None,
                        'roster': pages.get('roster') if 'pages' in locals() else None,
                        'home_shifts': pages.get('home_shifts') if 'pages' in locals() else None,
                        'away_shifts': pages.get('away_shifts') if 'pages' in locals() else None,
                        'summary': pages.get('summary') if 'pages' in locals() else None
                    }
                })
            i = i + 1
            retry_count = 0  # Reset for next game
            continue

        except KeyboardInterrupt:
            print('You manually interrupted the scrape. You will get to keep every game you have already completed scraping after just a bit of post-processing. Good bye.')
            global hidden_patrick
            hidden_patrick = 1
            # OPTIMIZED: Concat list to DataFrame
            full = pd.concat(full_list, ignore_index=True) if full_list else pd.DataFrame()
            if len(full) > 0:
                
                full = full.assign(home_skaters = np.where(~full.home_skaters.isin([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
                                                             (full.home_skaters.apply(lambda x: len(re.findall('[A-Z]', str(x)))) - 
                                                             full.home_skaters.apply(lambda x: len(re.findall('[G]', str(x))))),
                                                             full.home_skaters))

                full = full.assign(away_skaters = np.where(~full.away_skaters.isin([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
                                                             (full.away_skaters.apply(lambda x: len(re.findall('[A-Z]', str(x)))) - 
                                                             full.away_skaters.apply(lambda x: len(re.findall('[G]', str(x))))),
                                                             full.away_skaters))
                
                if 'away_on_1' in full.columns:
                
                    full = full.assign(
                    away_on_1 = np.where((pd.isna(full.away_on_1)) | (full.away_on_1 is None) | (full.away_on_1=='') | (full.away_on_1=='\xa0'), '\xa0', full.away_on_1),
                    away_on_2 = np.where((pd.isna(full.away_on_2)) | (full.away_on_2 is None) | (full.away_on_2=='') | (full.away_on_2=='\xa0'), '\xa0', full.away_on_2),
                    away_on_3 = np.where((pd.isna(full.away_on_3)) | (full.away_on_3 is None) | (full.away_on_3=='') | (full.away_on_3=='\xa0'), '\xa0', full.away_on_3),
                    away_on_4 = np.where((pd.isna(full.away_on_4)) | (full.away_on_4 is None) | (full.away_on_4=='') | (full.away_on_4=='\xa0'), '\xa0', full.away_on_4),
                    away_on_5 = np.where((pd.isna(full.away_on_5)) | (full.away_on_5 is None) | (full.away_on_5=='') | (full.away_on_5=='\xa0'), '\xa0', full.away_on_5),
                    away_on_6 = np.where((pd.isna(full.away_on_6)) | (full.away_on_6 is None) | (full.away_on_6=='') | (full.away_on_6=='\xa0'), '\xa0', full.away_on_6),
                    away_on_7 = np.where((pd.isna(full.away_on_7)) | (full.away_on_7 is None) | (full.away_on_7=='') | (full.away_on_7=='\xa0'), '\xa0', full.away_on_7),
                    away_on_8 = np.where((pd.isna(full.away_on_8)) | (full.away_on_8 is None) | (full.away_on_8=='') | (full.away_on_8=='\xa0'), '\xa0', full.away_on_8),
                    away_on_9 = np.where((pd.isna(full.away_on_9)) | (full.away_on_9 is None) | (full.away_on_9=='') | (full.away_on_9=='\xa0'), '\xa0', full.away_on_9),
                    home_on_1 = np.where((pd.isna(full.home_on_1)) | (full.home_on_1 is None) | (full.home_on_1=='') | (full.home_on_1=='\xa0'), '\xa0', full.home_on_1),
                    home_on_2 = np.where((pd.isna(full.home_on_2)) | (full.home_on_2 is None) | (full.home_on_2=='') | (full.home_on_2=='\xa0'), '\xa0', full.home_on_2),
                    home_on_3 = np.where((pd.isna(full.home_on_3)) | (full.home_on_3 is None) | (full.home_on_3=='') | (full.home_on_3=='\xa0'), '\xa0', full.home_on_3),
                    home_on_4 = np.where((pd.isna(full.home_on_4)) | (full.home_on_4 is None) | (full.home_on_4=='') | (full.home_on_4=='\xa0'), '\xa0', full.home_on_4),
                    home_on_5 = np.where((pd.isna(full.home_on_5)) | (full.home_on_5 is None) | (full.home_on_5=='') | (full.home_on_5=='\xa0'), '\xa0', full.home_on_5),
                    home_on_6 = np.where((pd.isna(full.home_on_6)) | (full.home_on_6 is None) | (full.home_on_6=='') | (full.home_on_6=='\xa0'), '\xa0', full.home_on_6),
                    home_on_7 = np.where((pd.isna(full.home_on_7)) | (full.home_on_7 is None) | (full.home_on_7=='') | (full.home_on_7=='\xa0'), '\xa0', full.home_on_7),
                    home_on_8 = np.where((pd.isna(full.home_on_8)) | (full.home_on_8 is None) | (full.home_on_8=='') | (full.home_on_8=='\xa0'), '\xa0', full.home_on_8),
                    home_on_9 = np.where((pd.isna(full.home_on_9)) | (full.home_on_9 is None) | (full.home_on_9=='') | (full.home_on_9=='\xa0'), '\xa0', full.home_on_9),
                    home_goalie = np.where((pd.isna(full.home_goalie)) | (full.home_goalie is None) | (full.home_goalie=='') | (full.home_goalie=='\xa0'), '\xa0', full.home_goalie),
                    away_goalie = np.where((pd.isna(full.away_goalie)) | (full.away_goalie is None) | (full.away_goalie=='') | (full.away_goalie=='\xa0'), '\xa0', full.away_goalie)
                    )
                
            # OPTIMIZED: Concat list to DataFrame before return
            full = pd.concat(full_list, ignore_index=True) if full_list else pd.DataFrame()
            # Clean up player_id column if present (used only for merge fallback)
            if 'player_id' in full.columns:
                full = full.drop(columns=['player_id'])
            if return_intermediates:
                return {'final': full, 'intermediates': intermediates_list}
            return full

    # OPTIMIZED: Concat list to DataFrame before final processing
    full = pd.concat(full_list, ignore_index=True) if full_list else pd.DataFrame()
    
    if len(full) > 0:
                
        full = full.assign(home_skaters = np.where(~full.home_skaters.isin([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
                                                             (full.home_skaters.apply(lambda x: len(re.findall('[A-Z]', str(x)))) - 
                                                             full.home_skaters.apply(lambda x: len(re.findall('[G]', str(x))))),
                                                             full.home_skaters))

        full = full.assign(away_skaters = np.where(~full.away_skaters.isin([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
                                                     (full.away_skaters.apply(lambda x: len(re.findall('[A-Z]', str(x)))) - 
                                                     full.away_skaters.apply(lambda x: len(re.findall('[G]', str(x))))),
                                                     full.away_skaters))

        if 'away_on_1' in full.columns:

            full = full.assign(
            away_on_1 = np.where((pd.isna(full.away_on_1)) | (full.away_on_1 is None) | (full.away_on_1=='') | (full.away_on_1=='\xa0'), '\xa0', full.away_on_1),
            away_on_2 = np.where((pd.isna(full.away_on_2)) | (full.away_on_2 is None) | (full.away_on_2=='') | (full.away_on_2=='\xa0'), '\xa0', full.away_on_2),
            away_on_3 = np.where((pd.isna(full.away_on_3)) | (full.away_on_3 is None) | (full.away_on_3=='') | (full.away_on_3=='\xa0'), '\xa0', full.away_on_3),
            away_on_4 = np.where((pd.isna(full.away_on_4)) | (full.away_on_4 is None) | (full.away_on_4=='') | (full.away_on_4=='\xa0'), '\xa0', full.away_on_4),
            away_on_5 = np.where((pd.isna(full.away_on_5)) | (full.away_on_5 is None) | (full.away_on_5=='') | (full.away_on_5=='\xa0'), '\xa0', full.away_on_5),
            away_on_6 = np.where((pd.isna(full.away_on_6)) | (full.away_on_6 is None) | (full.away_on_6=='') | (full.away_on_6=='\xa0'), '\xa0', full.away_on_6),
            away_on_7 = np.where((pd.isna(full.away_on_7)) | (full.away_on_7 is None) | (full.away_on_7=='') | (full.away_on_7=='\xa0'), '\xa0', full.away_on_7),
            away_on_8 = np.where((pd.isna(full.away_on_8)) | (full.away_on_8 is None) | (full.away_on_8=='') | (full.away_on_8=='\xa0'), '\xa0', full.away_on_8),
            away_on_9 = np.where((pd.isna(full.away_on_9)) | (full.away_on_9 is None) | (full.away_on_9=='') | (full.away_on_9=='\xa0'), '\xa0', full.away_on_9),
            home_on_1 = np.where((pd.isna(full.home_on_1)) | (full.home_on_1 is None) | (full.home_on_1=='') | (full.home_on_1=='\xa0'), '\xa0', full.home_on_1),
            home_on_2 = np.where((pd.isna(full.home_on_2)) | (full.home_on_2 is None) | (full.home_on_2=='') | (full.home_on_2=='\xa0'), '\xa0', full.home_on_2),
            home_on_3 = np.where((pd.isna(full.home_on_3)) | (full.home_on_3 is None) | (full.home_on_3=='') | (full.home_on_3=='\xa0'), '\xa0', full.home_on_3),
            home_on_4 = np.where((pd.isna(full.home_on_4)) | (full.home_on_4 is None) | (full.home_on_4=='') | (full.home_on_4=='\xa0'), '\xa0', full.home_on_4),
            home_on_5 = np.where((pd.isna(full.home_on_5)) | (full.home_on_5 is None) | (full.home_on_5=='') | (full.home_on_5=='\xa0'), '\xa0', full.home_on_5),
            home_on_6 = np.where((pd.isna(full.home_on_6)) | (full.home_on_6 is None) | (full.home_on_6=='') | (full.home_on_6=='\xa0'), '\xa0', full.home_on_6),
            home_on_7 = np.where((pd.isna(full.home_on_7)) | (full.home_on_7 is None) | (full.home_on_7=='') | (full.home_on_7=='\xa0'), '\xa0', full.home_on_7),
            home_on_8 = np.where((pd.isna(full.home_on_8)) | (full.home_on_8 is None) | (full.home_on_8=='') | (full.home_on_8=='\xa0'), '\xa0', full.home_on_8),
            home_on_9 = np.where((pd.isna(full.home_on_9)) | (full.home_on_9 is None) | (full.home_on_9=='') | (full.home_on_9=='\xa0'), '\xa0', full.home_on_9),
            home_goalie = np.where((pd.isna(full.home_goalie)) | (full.home_goalie is None) | (full.home_goalie=='') | (full.home_goalie=='\xa0'), '\xa0', full.home_goalie),
            away_goalie = np.where((pd.isna(full.away_goalie)) | (full.away_goalie is None) | (full.away_goalie=='') | (full.away_goalie=='\xa0'), '\xa0', full.away_goalie)
            )

        if live == True and 'game_strength_state' in full.columns:

            # Find the point in time where everybody jumps off (i.e., the synthetic shifts end) and get rid of that and everything after. 
            # (IF we have such a time)

            if len(
                full[(full.game_strength_state.str.contains('E')) & 
                    ((full.game_strength_state != 'EvE')) & 
                    (full.game_strength_state.shift(-1) == 'EvE') & 
                    (full.game_period == max(full.game_period))]) > 0:

                full = full[full.event_index <= 
                    full[(full.game_strength_state.str.contains('E')) & 
                        ((full.game_strength_state != 'EvE')) & 
                        (full.game_strength_state.shift(-1) == 'EvE') & 
                        (full.game_period == max(full.game_period))].event_index.iloc[-1] - 1]

            # If we don't have such a point in time (which can happen when home clock and away clock are misaligned, for example):
            # Then we find the final change and ditch everything beneath it

            elif full[full.event_type=='CHANGE'].iloc[-1].game_strength_state in ['5vE', 'Ev5']:

                full = full[full.event_index <= full[full.event_type=='CHANGE'].iloc[-1].event_index]

            if len(full[(full.event_type == 'SHOT') & 
                (((full.event_team==full.home_team) & (full.away_goalie=='\xa0')) | ((full.event_team==full.away_team) & (full.home_goalie=='\xa0')))
            ]) > 0:
                latest_bad_event = full[(full.event_type == 'SHOT') & 
                (((full.event_team==full.home_team) & (full.away_goalie=='\xa0')) | ((full.event_team==full.away_team) & (full.home_goalie=='\xa0')))
                ].event_index.min()
                full = full[full.event_index < latest_bad_event]

    # Clean up player_id column if present (used only for merge fallback)
    if 'player_id' in full.columns:
        full = full.drop(columns=['player_id'])

    if return_intermediates:
        return {'final': full, 'intermediates': intermediates_list}
    return full

def full_scrape(game_id_list, live = True, shift = False, return_intermediates = False, verbose = False):
    
    global hidden_patrick
    hidden_patrick = 0
    
    result = full_scrape_1by1(game_id_list, live, shift_to_espn = shift, return_intermediates = return_intermediates, verbose = verbose)
    
    # Handle return_intermediates case
    if return_intermediates:
        df = result['final']
        intermediates_list = result['intermediates']
    else:
        df = result
        intermediates_list = None
    
    if verbose:
        print('Full scrape complete, we have this many rows:', len(df))

    try:
        df = df.assign(
            event_player_1 = np.where(
                (df.event_player_1 == 'ELIAS PETTERSSON') & 
                (df.event_description.str.contains('#', na=False)) &
                (df.event_description.str.contains(' PETTERSSON', na=False)) &
                (df.event_description.str.extract(r'#(\d+) PETTERSSON', expand=False) == '25'), 
                'ELIAS PETTERSSON(D)', df.event_player_1),
            event_player_2 = np.where(
                (df.event_player_2 == 'ELIAS PETTERSSON') & 
                (
                    # Goal and Petey got A1
                    ((df.event_type == 'GOAL') &
                    (df.event_description.str.contains(': #', na=False)) &
                    (df.event_description.str.contains(' PETTERSSON', na=False)) &
                    (df.event_description.str.extract(r': #(\d+) PETTERSSON', expand=False) == '25')) |
                    # Not a goal, Petey was EP2
                    ((df.event_type != 'GOAL') & 
                    (df.event_description.str.contains('VAN #', na=False)) &
                    (df.event_description.str.contains(' PETTERSSON', na=False)) &
                    (df.event_description.str.extract(r'VAN #(\d+) PETTERSSON', expand=False) == '25'))
                ),
                'ELIAS PETTERSSON(D)', df.event_player_2),
            event_player_3 = np.where(
                (df.event_player_3=='ELIAS PETTERSSON') & 
                (df.event_description.str.contains('#', na=False)) &
                (df.event_description.str.contains(' PETTERSSON', na=False)) &
                (df.event_description.str.extract(r'#(\d+) PETTERSSON(?:\s|$)', expand=False) == '25'),
                'ELIAS PETTERSSON(D)', df.event_player_3)
        )
    except Exception as e:
        print(e)

    # Don't even need this, we've had this problem with Stutzle for years, just let it be. 
    # df.event_description = df.event_description.str.replace('FEHÃ\x89RVÃ\x81RY', 'FEHERVARY').str.replace('BLÃMEL', 'BLAMEL')
    
    if (hidden_patrick==0) and (len(df)>0):
        
        gids = list(set(df.game_id))
        missing = [x for x in game_id_list if x not in gids]
        if len(missing)>0:
            print('You missed the following games: ' + str(missing))
            print('Let us try scraping each of them one more time.')
            retry_result = full_scrape_1by1(missing, return_intermediates = return_intermediates, verbose = verbose)
            if return_intermediates:
                retry_df = retry_result['final']
                retry_intermediates = retry_result['intermediates']
                df = pd.concat([df, retry_df], ignore_index=True)
                intermediates_list.extend(retry_intermediates)
                return {'final': df, 'intermediates': intermediates_list}
            else:
                df = pd.concat([df, retry_result], ignore_index=True)
                return df
        else:
            if return_intermediates:
                return {'final': df, 'intermediates': intermediates_list}
            return df
    
    else:
        if return_intermediates:
            return {'final': df, 'intermediates': intermediates_list}
        return df

print("Welcome to the TopDownHockey NHL Scraper, built by Patrick Bacon.")
print("If you enjoy the scraper and would like to support my work, or you have any comments, questions, or concerns, feel free to follow me on Twitter @TopDownHockey or reach out to me via email at patrick.s.bacon@gmail.com. Have fun!")