"""

A package built for scraping Elite Prospects

This package is built for personal use. If you are interested in professional use, look into the EliteProspects API.
"""

import numpy as np
import pandas as pd
from bs4  import BeautifulSoup
import requests
import time
from datetime import datetime 
import warnings
warnings.filterwarnings("ignore")
import sys
from requests import ConnectionError, ReadTimeout, ConnectTimeout, HTTPError, Timeout

def tableDataText(table):

    """
    A function that is built strictly for the back end and should not be run by the user.
    Function built by Marcus Sjölin
    """

    rows = []
    trs = table.find_all('tr')

    headerow = [td.get_text(strip=True) for td in trs[0].find_all('th')] # header row
    if headerow: # if there is a header row include first
        rows.append(headerow)
        trs = trs[1:]
    for tr in trs: # for every table row
        rows.append([td.get_text(strip=True) for td in tr.find_all('td')]) # data row
        
    df_rows = pd.DataFrame(rows[1:], columns=rows[0])
    
    return df_rows

def getskaters(league, year):  
    """
    A function that is built strictly for the back end and should not be run by the user.
    """
   
    url = 'https://www.eliteprospects.com/league/' + league + '/stats/' + year + '?page='
    # print('Collects data from ' + 'https://www.eliteprospects.com/league/' + league + '/stats/' + year)
    
    print("Beginning scrape of " + league + " skater data from " + year + ".")
    
    # Return list with all plyers for season in link     
    players = []
    
    page = (requests.get(url+str(1), timeout = 500))
    first_page_string = str(page)
    
    while first_page_string == '<Response [403]>':
        print("Just got a 403 Error before entering the page. Time to Sleep, then re-obtain the link.")
        time.sleep(100)
        page = (requests.get(url+str(1), timeout = 500))
        first_page_string = str(page)
        print("Changed the string before entering the page. Let's try again")
    
    if (str(first_page_string) == '<Response [404]>'):
        print("ERROR: " + str(first_page_string) + " on league: " + league + " in year: " + year + ". Data doesn't exist for this league in this year.")
    
    else:
        
        for i in range(1,99):
            page = requests.get(url+str(i), timeout = 500) 
            page_string = str(page)
            
            while page_string == '<Response [403]>':
                print("Just got a 403 Error within the page. Time to Sleep, then re-obtain the link.")
                time.sleep(100)
                page = requests.get(url+str(i), timeout = 500) 
                page_string = str(page)
                print("Changed the string within the page. Let's try again")
                
            soup = BeautifulSoup(page.content, "html.parser")

            # Get data for players table
            player_table = soup.find( "table", {"class":"table table-striped table-sortable player-stats highlight-stats season"})
            
            try:
                df_players = tableDataText(player_table)
                
            except AttributeError:
                print("BREAK: TABLE NONE ERROR: " + str(requests.get(url+str(i), timeout = 500)) + " On League: " + league + " In Year: " + year)
                break
                
            if len(df_players)>0:

                if df_players['#'].count()>0:
                    # Remove empty rows
                    df_players = df_players[df_players['#']!=''].reset_index(drop=True)

                    # Extract href links in table
                    href_row = []
                    for link in player_table.find_all('a'):
                        href_row.append(link.attrs['href'])

                    # Create data frame, rename and only keep links to players
                    df_links = pd.DataFrame(href_row)  
                    df_links.rename(columns={ df_links.columns[0]:"link"}, inplace=True)
                    df_links= df_links[df_links['link'].str.contains("/player/")].reset_index(drop=True)    

                    # Add links to players
                    df_players['link']=df_links['link'] 

                    players.append(df_players)

                    # Wait 3 seconds before going to next
                    #time.sleep(1)
                    #print("Scraped page " + str(i))
                    
            else:
                #print("Scraped final page of: " + league + " In Year: " + year)
                break

    
        if len(players)!=0:
            df_players = pd.concat(players).reset_index()

            df_players.columns = map(str.lower, df_players.columns)

            # Clean up dataset
            df_players['season'] = year
            df_players['league'] = league

            df_players = df_players.drop(['index','#'], axis=1).reset_index(drop=True)

            df_players['playername'] = df_players['player'].str.replace(r"\(.*\)","")
            df_players['position'] = df_players['player'].str.extract('.*\((.*)\).*')
            df_players['position'] = np.where(pd.isna(df_players['position']), "F", df_players['position'])

            df_players['fw_def'] = df_players['position'].str.contains('LW|RW|C|F')
            df_players.loc[df_players['position'].str.contains('LW|RW|C'), 'fw_def'] = 'FW'
            df_players.loc[df_players['position'].str.contains('D'), 'fw_def'] = 'DEF'

            # Adjust columns; transform data
            team = df_players['team'].str.split("“", n=1, expand=True)
            df_players['team'] = team[0]

            # drop player-column
            df_players = df_players.drop(columns = ['fw_def'], axis=1)
            print("Successfully scraped all " + league + " skater data from " + year + ".")

            return df_players
        
        else: print("LENGTH 0 ERROR: " + str(requests.get(url+str(1), timeout = 500)) + " On League: " + league + " In Year: " + year)
            
def getgoalies(league, year):
    """
    A function that is built strictly for the back end and should not be run by the user.
    """

    url = 'https://www.eliteprospects.com/league/' + league + '/stats/' + year + '?page-goalie='
    # print('Collects data from ' + 'https://www.eliteprospects.com/league/' + league + '/stats/' + year)
    
    print("Beginning scrape of " + league + " goalie data from " + year + ".")
    
    # Return list with all plyers for season in link     
    players = []
    
    page = (requests.get(url + str(1) + "#goalies", timeout = 500))
    first_page_string = str(page)
    
    while first_page_string == '<Response [403]>':
        print("Just got a 403 Error before entering the page. This means EliteProspects has temporarily blocked your IP address.")
        print("We're going to sleep for 60 seconds, then try again.")
        time.sleep(100)
        page = (requests.get(url + str(1) + "#goalies", timeout = 500))
        first_page_string = str(page)
        print("Okay, let's try this again")
    
    if (first_page_string) == '<Response [404]>':
        print("ERROR: " + first_page_string + " on league: " + league + " in year: " + year + ". Data doesn't exist for this league and season.")
    
    else:
        
        for i in range(1,99):
            page = requests.get(url+str(i), timeout = 500)
            page_string = str(page)
            
            while page_string == '<Response [403]>':
                print("Just got a 403 Error within the page. Time to Sleep, then re-obtain the link.")
                time.sleep(100)
                page = (requests.get(url+str(i), timeout = 500))
                page_string = str(page)
                print("Changed the string within the page. Let's try again")
                
            soup = BeautifulSoup(page.content, "html.parser")

            # Get data for players table
            player_table = soup.find("table", {"class":"table table-striped table-sortable goalie-stats highlight-stats season"})

            try:
                df_players = tableDataText(player_table)
            except AttributeError:
                print("BREAK: TABLE NONE ERROR: " + str(requests.get(url+str(i), timeout = 500)) + " On League: " + league + " In Year: " + year)
                break
                
            if len(df_players)>0:

                if df_players['#'].count()>0:
                    # Remove empty rows
                    df_players = df_players[df_players['#']!=''].reset_index(drop=True)

                    # Extract href links in table
                    href_row = []
                    for link in player_table.find_all('a'):
                        href_row.append(link.attrs['href'])

                    # Create data frame, rename and only keep links to players
                    df_links = pd.DataFrame(href_row)  
                    df_links.rename(columns={ df_links.columns[0]:"link"}, inplace=True)
                    df_links= df_links[df_links['link'].str.contains("/player/")].reset_index(drop=True)    

                    # Add links to players
                    df_players['link']=df_links['link'] 

                    players.append(df_players)

                    # Wait 3 seconds before going to next
                    # time.sleep(1)
                    #print("Scraped page " + str(i))
                    
            else:
                #print("Scraped final page of: " + league + " In Year: " + year)
                break

    
        if len(players)!=0:
            df_players = pd.concat(players).reset_index()

            df_players.columns = map(str.lower, df_players.columns)

            # Clean up dataset
            df_players['season'] = year
            df_players['league'] = league

            df_players = df_players.drop(['index','#'], axis=1).reset_index(drop=True)
            
            print("Successfully scraped all " + league + " goalie data from " + year + ".")
            
            df_players = df_players.loc[((df_players.gp!=0) & (~pd.isna(df_players.gp)) & (df_players.gp!="0") & (df_players.gaa!="-"))]

            return df_players
        else: print("LENGTH 0 ERROR: " + str(requests.get(url+str(1), timeout = 500)) + " On League: " + league + " In Year: " + year)  
    
def get_info(link):
    """
    A function that is built strictly for the back end and should not be run by the user.
    """

    page = requests.get(link, timeout = 500)
    soup = BeautifulSoup(page.content, "html.parser")

    page_string = str(page)

    while ((page_string == '<Response [403]>') or ("evil" in str(soup.p))): 
        print("403 Error. re-obtaining string and re-trying.")
        page = requests.get(link, timeout = 500)
        page_string = str(page)
        soup = BeautifulSoup(page.content, "html.parser")
        time.sleep(60)

    lis = soup.find_all('li')

    relevant_lis = [li for li in lis if li.find('span') is not None]

    # player
    
    if soup.find("title") != None:
        player = soup.find("title").string.replace(' - Stats, Contract, Salary & More', '')
    else:
        player = '-'
    
    # status
    
    if [li for li in relevant_lis if li.find('span').text=='NHL Rights'] != []:
        rights = [li for li in relevant_lis if li.find('span').text=='NHL Rights'][0].find('a').text.split(' /')[0]
    else:
        rights = '-'
    
    # rights
    
    if [li for li in relevant_lis if li.find('span').text=='NHL Rights'] != []:
        status = [li for li in relevant_lis if li.find('span').text=='NHL Rights'][0].find('a').text.split(' / ')[1]
    else:
        status = '-'
    
    # dob
    
    if [li.find('a')['href'].split('dob=', 1)[1].split('&sort', 1)[0] for li in relevant_lis if li.span.text == 'Date of Birth'] != []:
        dob = [li.find('a')['href'].split('dob=', 1)[1].split('&sort', 1)[0] for li in relevant_lis if li.span.text == 'Date of Birth'][0]
    else:
        dob = '-'
    
    # height
    
    if [li for li in relevant_lis if li.find('span').text=='Height'] != []:
        height = [li for li in relevant_lis if li.find('span').text=='Height'][0].text.split('Height')[1].split(' cm')[0]
    else: 
        height = '-'
    
    # weight
    
    if [li for li in relevant_lis if li.find('span').text=='Weight'] != []:
        weight = [li for li in relevant_lis if li.find('span').text=='Weight'][0].text.split('Weight')[1].split(' cm')[0]
    else: 
        weight = '-'
    
    # birthplace
    
    if [li for li in relevant_lis if li.find('span').text=='Place of Birth'] != []:
        birthplace = [li for li in relevant_lis if li.find('span').text=='Place of Birth'][0].text.split('Birth')[1]
    else:
        birthplace = '-'
    
    # nation
    
    if [li for li in relevant_lis if li.find('span').text=='Nation'] != []:
        nation = [li for li in relevant_lis if li.find('span').text=='Nation'][0].text.split('Nation')[1]
    else:
        nation = '-'
    
    # shoots
    
    if [li for li in relevant_lis if li.find('span').text=='Shoots'] != []:
        shoots = [li for li in relevant_lis if li.find('span').text=='Shoots'][0].text.split('Shoots')[1]
    else:
        shoots = '-'
    
    # draft
    
    if [li for li in relevant_lis if li.find('span').text=='Drafted'] != []:
        draft = [li for li in relevant_lis if li.find('span').text=='Drafted'][0].text.split('Drafted')[1]
    else:
        draft = '-'
    
    return(player, rights, status, dob, height, weight, birthplace, nation, shoots, draft, link)  
    
def get_player_information(dataframe):
    '''
    Takes a data frame from the get_players or get_goalies function and obtains biographcal information for all players in said dataframe, then returns it as a dataframe.
    '''

    myplayer = []
    myrights = []
    mystatus = []
    mydob = []
    myheight = []
    myweight = []
    mybirthplace = []
    mynation = []
    myshot = []
    mydraft = []
    mylink = []
    
    print("Beginning scrape for " + str(len(list(set(dataframe.link)))) + " players.")

    for i in range(0, len(list(set(dataframe.link)))):
        try:
            myresult = get_info(((list(set(dataframe.link))[i])))
            myplayer.append(myresult[0])
            myrights.append(myresult[1])
            mystatus.append(myresult[2])
            mydob.append(myresult[3])
            myheight.append(myresult[4])
            myweight.append(myresult[5])
            mybirthplace.append(myresult[6])
            mynation.append(myresult[7])
            myshot.append(myresult[8])
            mydraft.append(myresult[9])
            mylink.append(myresult[10])
            print(myresult[0] + " scraped! That's " + str(i + 1) + " down! Only " + str(len(list(set(dataframe.link))) - (i + 1)) +  " left to go!")
        except KeyboardInterrupt:
            print("You interrupted this one manually. The output here will be every player you've scraped so far. Good bye!")
            break
        except (ConnectionError,
            HTTPError,
            ReadTimeout,
            ConnectTimeout,
            ValueError) as errormessage:
            print("You've been disconnected. Here's the error message:")
            print(errormessage)
            print("Luckily, everything you've scraped up to this point will still be safe.")
            break

    resultdf = pd.DataFrame(columns = ["player", "rights", "status", "dob", "height", "weight", "birthplace", "nation", "shoots", "draft", "link"])

    resultdf.player = myplayer
    resultdf.rights = myrights
    resultdf.status = mystatus
    resultdf.dob = mydob
    resultdf.height = myheight
    resultdf.weight = myweight
    resultdf.birthplace = mybirthplace
    resultdf.nation = mynation
    resultdf.shoots = myshot
    resultdf.draft = mydraft
    resultdf.link = mylink
    
    print("Your scrape is complete! You've obtained player information for " + str(len(resultdf)) + " players!")
    
    return resultdf
        
def get_league_skater_boxcars(league, seasons):
    """
    A function that is built strictly for the back end and should not be run by the user.
    """

    if len(set(seasons))==1:
        scraped_season_list = str(seasons)
    elif len(set(seasons))>2:
        scraped_season_list = str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + ", and " + str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
    else:
        scraped_season_list = str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + " and " + str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
    
    
    global hidden_patrick
    hidden_patrick = 0
    global error
    error = 0
    
    output = pd.DataFrame()
    
    if type(seasons) == str:
        single = getskaters(league, seasons)
        output = output._append(single)
        print("Scraping " + league + " data is complete. You scraped skater data from " + seasons + ".")
        return(output)
    
    elif ((type(seasons) == tuple) or (type(seasons) == list)):
    
        for i in range(0, len(seasons)):
            try:
                single = getskaters(league, seasons[i])
                output = output._append(single)
            except KeyboardInterrupt as e:
                hidden_patrick = 4
                error = e
                return output
            except (ConnectionError,
                HTTPError,
                ReadTimeout,
                ConnectTimeout,
                ValueError) as e:
                hidden_patrick = 5
                error = e
                return output
            
        print("Scraping " + league + " data is complete. You scraped skater data from " + scraped_season_list + ".")    
        return(output)
    
def get_league_goalie_boxcars(league, seasons):
    """
    A function that is built strictly for the back end and should not be run by the user.
    """

    if len(set(seasons))==1:
        scraped_season_list = str(seasons)
    elif len(set(seasons))>2:
        scraped_season_list = str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + ", and " + str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
    else:
        scraped_season_list = str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + " and " + str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
    
    
    global hidden_patrick
    hidden_patrick = 0
    global error
    error = 0
    
    output = pd.DataFrame()
    
    if type(seasons) == str:
        single = getgoalies(league, seasons)
        output = output._append(single)
        print("Scraping " + league + " data is complete. You scraped goalie data from " + seasons + ".")
        return(output)
    
    elif ((type(seasons) == tuple) or (type(seasons) == list)):
    
        for i in range(0, len(seasons)):
            try:
                single = getgoalies(league, seasons[i])
                output = output._append(single)
            except KeyboardInterrupt as e:
                hidden_patrick = 4
                error = e
                return output
            except (ConnectionError,
                HTTPError,
                ReadTimeout,
                ConnectTimeout) as e:
                hidden_patrick = 5
                error = e
                return output
            
        print("Scraping " + league + " data is complete. You scraped goalie data from " + scraped_season_list + ".")    
        return(output)

def get_goalies(leagues, seasons):
    '''
    Obtains goalie data for at least one season and at least one league. Returns a dataframe.
    '''
    
    if (len(seasons)==1 or type(seasons)==str):
        season_string = str(seasons)
    elif len(seasons)==2:
        season_string = " and".join(str((tuple(sorted(tuple(seasons))))).replace("'", "").replace("(", "").replace(")", "").split(","))
    else:
        season_string = str(((tuple(sorted(tuple(seasons)))))[:-1]).replace("'", "").replace("(", "").replace(")", "") + " and " + str(((tuple(sorted(tuple(seasons)))))[-1])
        
    if (len(leagues)==1 or type(leagues)==str):
        league_string = str(leagues)
    elif len(leagues)==2:
        league_string = " and".join(str((tuple(sorted(tuple(leagues))))).replace("'", "").replace("(", "").replace(")", "").split(","))
    else:
        league_string = str(((tuple(sorted(tuple(leagues)))))[:-1]).replace("'", "").replace("(", "").replace(")", "") + " and " + str(((tuple(sorted(tuple(leagues)))))[-1])
    
    leaguesall = pd.DataFrame()

    if ((type(leagues)==str) and (type(seasons)==str)):
        print("Your scrape request is goalie data from the following league:")
        print(league_string)
        print("In the following season:")
        print(season_string)
        leaguesall = get_league_goalie_boxcars(leagues, seasons)
        print("Completed scraping goalie data from the following league:")
        print(str(leagues))
        print("Over the following season:")
        print(str(seasons))
        return(leaguesall.reset_index().drop(columns = 'index')) 
        
    elif ((type(leagues)==str) and ((type(seasons) == tuple) or (type(seasons) == list))):
        print("Your scrape request is goalie data from the following league:")
        print(league_string)
        print("In the following seasons:")
        print(season_string)
        leaguesall = get_league_goalie_boxcars(leagues, seasons)
        
        if hidden_patrick == 4:
            print("You interrupted this one manually. The output here will be every player you've scraped so far. Good bye!")
            return(leaguesall.reset_index().drop(columns = 'index')) 
        if hidden_patrick == 5:
            print("You were disconnected! The output here will be every player you've scraped so far. Here's your error message:")
            print(error)
            return(leaguesall.reset_index().drop(columns = 'index')) 
        
        if len(set(leaguesall.league))==1:
            scraped_league_list = leaguesall.league
        elif len(set(leaguesall.league))>2:
            scraped_league_list = str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + ", and " + str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        else:
            scraped_league_list = str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + " and " + str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        
        if len(set(seasons))==1:
            scraped_season_list = seasons
        elif len(set(seasons))>2:
            scraped_season_list = str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + ", and " + str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        else:
            scraped_season_list = str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + " and " + str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        
        print("Completed scraping goalie data from the following league:")
        print(str(leagues))
        print("Over the following seasons:")
        print(scraped_season_list)
        return(leaguesall.reset_index().drop(columns = 'index'))    
    
    elif ((type(seasons) == str) and ((type(leagues) == tuple) or (type(leagues) == list))):
        print("Your scrape request is goalie data from the following leagues:")
        print(league_string)
        print("In the following season:")
        print(season_string)
        
        for i in range (0, len(leagues)):
            try:
                targetleague = get_league_goalie_boxcars(leagues[i], seasons)
                leaguesall = leaguesall._append(targetleague)
                if hidden_patrick == 4:
                    raise KeyboardInterrupt
                if hidden_patrick == 5:
                    raise ConnectionError
            except KeyboardInterrupt:
                print("You interrupted this one manually. The output here will be every player you've scraped so far. Good bye!")
                break
            except ConnectionError:
                print("You were disconnected! Let's sleep and try again.")
                print(error)
                time.sleep(100)
                continue
                
        if len(set(leaguesall.league))==1:
            scraped_league_list = leaguesall.league
        elif len(set(leaguesall.league))>2:
            scraped_league_list = str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + ", and " + str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        else:
            scraped_league_list = str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + " and " + str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        
        print("Completed scraping goalie data from the following leagues:")
        print(scraped_league_list)
        print("Over the following season:")
        print((seasons))
        return(leaguesall.reset_index().drop(columns = 'index'))    
            
    elif (((type(seasons) == tuple) or (type(seasons) == list)) and ((type(leagues) == tuple) or (type(leagues) == list))):
        print("Your scrape request is goalie data from the following leagues:")
        print(league_string)
        print("In the following seasons:")
        print(season_string)
        #print("Your scrape request: " + str(leagues[:-1]).replace("'", "").replace("(", "").replace(")", "") + ", and " + (leagues)[-1] + " goalie data from " +str(seasons[:-1]).replace("'", "").replace("(", "").replace(")", "") + ", and " + (seasons)[-1] + ".")    
        for i in range (0, len(leagues)):
            try:
                targetleague = get_league_goalie_boxcars(leagues[i], seasons)
                leaguesall = leaguesall._append(targetleague)
                if hidden_patrick == 4:
                    raise KeyboardInterrupt
                if hidden_patrick == 5:
                    raise ConnectionError
            except KeyboardInterrupt:
                print("You interrupted this one manually. The output here will be every player you've scraped so far. Good bye!")
                break
            except ConnectionError:
                print("You were disconnected! Let's sleep and try again.")
                print(error)
                time.sleep(100)
                continue
                
        if len(set(leaguesall.league))==1:
            scraped_league_list = leaguesall.league
        elif len(set(leaguesall.league))>2:
            scraped_league_list = str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + ", and " + str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        else:
            scraped_league_list = str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + " and " + str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        
        if len(set(seasons))==1:
            scraped_season_list = seasons
        elif len(set(seasons))>2:
            scraped_season_list = str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + ", and " + str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        else:
            scraped_season_list = str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + " and " + str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        
        print("Completed scraping goalie data from the following leagues:")
        print(scraped_league_list)
        print("Over the following seasons:")
        print(scraped_season_list)
        return(leaguesall.reset_index().drop(columns = 'index'))        
                
    else:
        print("There was an issue with the request you made. Please enter a single league and season as a string, or multiple leagues as either a list or tuple.")
    
    
def get_skaters(leagues, seasons):

    '''
    Obtains skater data for at least one season and at least one league. Returns a dataframe.
    '''
    
    if (len(seasons)==1 or type(seasons)==str):
        season_string = str(seasons)
    elif len(seasons)==2:
        season_string = " and".join(str((tuple(sorted(tuple(seasons))))).replace("'", "").replace("(", "").replace(")", "").split(","))
    else:
        season_string = str(((tuple(sorted(tuple(seasons)))))[:-1]).replace("'", "").replace("(", "").replace(")", "") + " and " + str(((tuple(sorted(tuple(seasons)))))[-1])
        
    if (len(leagues)==1 or type(leagues)==str):
        league_string = str(leagues)
    elif len(leagues)==2:
        league_string = " and".join(str((tuple(sorted(tuple(leagues))))).replace("'", "").replace("(", "").replace(")", "").split(","))
    else:
        league_string = str(((tuple(sorted(tuple(leagues)))))[:-1]).replace("'", "").replace("(", "").replace(")", "") + " and " + str(((tuple(sorted(tuple(leagues)))))[-1])
    
    leaguesall = pd.DataFrame()

    if ((type(leagues)==str) and (type(seasons)==str)):
        print("Your scrape request is skater data from the following league:")
        print(league_string)
        print("In the following season:")
        print(season_string)
        leaguesall = get_league_skater_boxcars(leagues, seasons)
        print("Completed scraping skater data from the following league:")
        print(str(leagues))
        print("Over the following season:")
        print(str(seasons))
        return(leaguesall.reset_index().drop(columns = 'index')) 
        
    elif ((type(leagues)==str) and ((type(seasons) == tuple) or (type(seasons) == list))):
        print("Your scrape request is skater data from the following league:")
        print(league_string)
        print("In the following seasons:")
        print(season_string)
        leaguesall = get_league_skater_boxcars(leagues, seasons)
        
        if hidden_patrick == 4:
            print("You interrupted this one manually. The output here will be every player you've scraped so far. Good bye!")
            return(leaguesall.reset_index().drop(columns = 'index')) 
        if hidden_patrick == 5:
            print("You were disconnected! The output here will be every player you've scraped so far. Here's your error message:")
            print(error)
            return(leaguesall.reset_index().drop(columns = 'index')) 
        
        if len(set(leaguesall.league))==1:
            scraped_league_list = leaguesall.league
        elif len(set(leaguesall.league))>2:
            scraped_league_list = str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + ", and " + str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        else:
            scraped_league_list = str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + " and " + str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        
        if len(set(seasons))==1:
            scraped_season_list = seasons
        elif len(set(seasons))>2:
            scraped_season_list = str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + ", and " + str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        else:
            scraped_season_list = str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + " and " + str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        
        print("Completed scraping skater data from the following league:")
        print(str(leagues))
        print("Over the following seasons:")
        print(scraped_season_list)
        return(leaguesall.reset_index().drop(columns = 'index'))    
    
    elif ((type(seasons) == str) and ((type(leagues) == tuple) or (type(leagues) == list))):
        print("Your scrape request is skater data from the following leagues:")
        print(league_string)
        print("In the following season:")
        print(season_string)
        
        for i in range (0, len(leagues)):
            try:
                targetleague = get_league_skater_boxcars(leagues[i], seasons)
                leaguesall = leaguesall._append(targetleague)
                if hidden_patrick == 4:
                    raise KeyboardInterrupt
                if hidden_patrick == 5:
                    raise ConnectionError
            except KeyboardInterrupt:
                print("You interrupted this one manually. The output here will be every player you've scraped so far. Good bye!")
                break
            except ConnectionError:
                print("You were disconnected! Let's sleep and try again.")
                print(error)
                time.sleep(100)
                continue
                
        if len(set(leaguesall.league))==1:
            scraped_league_list = leaguesall.league
        elif len(set(leaguesall.league))>2:
            scraped_league_list = str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + ", and " + str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        else:
            scraped_league_list = str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + " and " + str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        
        print("Completed scraping skater data from the following leagues:")
        print(scraped_league_list)
        print("Over the following season:")
        print((seasons))
        return(leaguesall.reset_index().drop(columns = 'index'))    
            
    elif (((type(seasons) == tuple) or (type(seasons) == list)) and ((type(leagues) == tuple) or (type(leagues) == list))):
        print("Your scrape request is skater data from the following leagues:")
        print(league_string)
        print("In the following seasons:")
        print(season_string)
        #print("Your scrape request: " + str(leagues[:-1]).replace("'", "").replace("(", "").replace(")", "") + ", and " + (leagues)[-1] + " skater data from " +str(seasons[:-1]).replace("'", "").replace("(", "").replace(")", "") + ", and " + (seasons)[-1] + ".")    
        for i in range (0, len(leagues)):
            try:
                targetleague = get_league_skater_boxcars(leagues[i], seasons)
                leaguesall = leaguesall._append(targetleague)
                if hidden_patrick == 4:
                    raise KeyboardInterrupt
                if hidden_patrick == 5:
                    raise ConnectionError
            except KeyboardInterrupt:
                print("You interrupted this one manually. The output here will be every player you've scraped so far. Good bye!")
                break
            except ConnectionError:
                print("You were disconnected! Let's sleep and try again.")
                print(error)
                time.sleep(100)
                continue
                
        if len(set(leaguesall.league))==1:
            scraped_league_list = leaguesall.league
        elif len(set(leaguesall.league))>2:
            scraped_league_list = str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + ", and " + str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        else:
            scraped_league_list = str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + " and " + str(((str(list(set(leaguesall.league))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        
        if len(set(seasons))==1:
            scraped_season_list = seasons
        elif len(set(seasons))>2:
            scraped_season_list = str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + ", and " + str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        else:
            scraped_season_list = str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[:-1]).replace("'", "").replace("[", "").replace("]", "") + " and " + str(((str(tuple(sorted(tuple(set(seasons))))).replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", ""))).split(", ")[-1])
        
        print("Completed scraping skater data from the following leagues:")
        print(scraped_league_list)
        print("Incorporating the following seasons:")
        print(scraped_season_list)
        return(leaguesall.reset_index().drop(columns = 'index'))        
                
    else:
        print("There was an issue with the request you made. Please enter a single league and season as a string, or multiple leagues as either a list or tuple.")

def add_player_information(dataframe):
    '''
    Takes a data frame from the get_players or get_goalies function and obtains biographcal information for all players in said dataframe, then returns it as a dataframe that adds to the other data you've already scraped..
    '''
    with_player_info = get_player_information(dataframe)
    doubledup = dataframe.merge(with_player_info.drop(columns = ['player']), on = 'link', how = 'inner')
    return doubledup

### EXAMPLE ONE: GET ALL SKATERS FROM THE MHL IN 2020-2021 ###

#mhl2021 = get_skaters("mhl", "2020-2021")
print("Welcome to the TopDownHockey EliteProspects Scraper, built by Patrick Bacon.")
print("This scraper is built strictly for personal use. For commercial or professional use, please look into the EliteProspects API.")
print("If you enjoy the scraper and would like to support my work, feel free to follow me on Twitter @TopDownHockey. Have fun!")

