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

class Patrick(Exception): pass

def tableDataText(table):
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
    Get all players for specific year and league; returns dataframe
    League input in format '2018-19'
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
                print("BREAK: TABLE NONE ERROR: " + str(requests.get(url+str(i)), timeout = 500) + " On League: " + league + " In Year: " + year)
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
        
        else: print("LENGTH 0 ERROR: " + str(requests.get(url+str(1)), timeout = 500) + " On League: " + league + " In Year: " + year)
            
def getgoalies(league, year):

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
                print("BREAK: TABLE NONE ERROR: " + str(requests.get(url+str(i)), timeout = 500) + " On League: " + league + " In Year: " + year)
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
    
    page = requests.get(link, timeout = 500)
    soup = BeautifulSoup(page.content, "html.parser")

    page_string = str(page)

    while ((page_string == '<Response [403]>') or ("evil" in str(soup.p))): 
        print("403 Error. re-obtaining string and re-trying.")
        page = requests.get(link, timeout = 500)
        page_string = str(page)
        soup = BeautifulSoup(page.content, "html.parser")
        time.sleep(60)

    if soup.find("title") != None:
        player = soup.find("title").string.replace(" - Elite Prospects" ,"")

    else: player = "-"

    if (soup.find("div", {"class":"col-xs-12 col-17 text-right p-0 ep-text-color--black"}))!= None:
        if 'dob' in (soup.find("div", {"class":"col-xs-12 col-17 text-right p-0 ep-text-color--black"})).find("a")['href']:
            dob = soup.find("div", {"class":"col-xs-12 col-17 text-right p-0 ep-text-color--black"}).find("a")['href'].split("dob=", 1)[1].split("&sort", 1)[0]
        else: 
            dob = "-"

    else:
        dob = "-"

    if soup.find("div", {"class":"order-6 order-sm-3 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}) != None:
        if "cm" in soup.find("div", {"class":"order-6 order-sm-3 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}
                            ).find(
        "div", {"class":"col-xs-12 col-18 text-right p-0 ep-text-color--black"}).string:
            height = soup.find("div", {"class":"order-6 order-sm-3 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}
                              ).find(
            "div", {"class":"col-xs-12 col-18 text-right p-0 ep-text-color--black"}).string.split(" / ")[1].split("cm")[0].strip()
        else: 
            height = "-"

    else: 
        height = "-"

    if soup.find("div", {"class":"order-7 order-sm-5 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}) != None:
        if soup.find("div", {"class":"order-7 order-sm-5 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}
                    ).find(
            "div", {"class":"col-xs-12 col-18 text-right p-0 ep-text-color--black"}).string.split("\n")[1].split("lbs")[0].strip() == '- / -':
                weight = "-"
        else: 
            weight = soup.find("div", {"class":"order-7 order-sm-5 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}
                              ).find(
            "div", {"class":"col-xs-12 col-18 text-right p-0 ep-text-color--black"}).string.split("\n")[1].split("lbs")[0].strip()

    else: weight = "-"

    if soup.find("div", {"class":"order-2 order-sm-4 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}
                         ) != None:
        if soup.find("div", {"class":"order-2 order-sm-4 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}
                         ).find(
            "div", {"class":"col-xs-12 col-17 text-right p-0 ep-text-color--black"}).find("a") != None:

            birthplace = soup.find("div", {"class":"order-2 order-sm-4 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}
                         ).find(
                        "div", {"class":"col-xs-12 col-17 text-right p-0 ep-text-color--black"}).find("a").string.replace("\n", "").strip()

        else: 
            birthplace = "-"
    else: 
        birthplace = "-"

    if soup.find("div", {"class":"order-3 order-sm-6 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}) != None:
        if soup.find("div", {"class":"order-3 order-sm-6 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}
                    ).find(
            "div", {"class":"col-xs-12 col-18 text-right p-0 ep-text-color--black"}).find("a") != None:
                nation = soup.find("div", {"class":"order-3 order-sm-6 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}
                                  ).find(
                "div", {"class":"col-xs-12 col-18 text-right p-0 ep-text-color--black"}).find("a").string.replace("\n", "").strip()
        else: nation = "-"

    else:
        nation = "-"

    if soup.find("div", {"class":"order-8 order-sm-7 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}) !=None:
        shoots = soup.find("div", {"class":"order-8 order-sm-7 ep-list__item ep-list__item--col-2 ep-list__item--in-card-body ep-list__item--is-compact"}
                          ).find(
            "div", {"class":"col-xs-12 col-18 text-right p-0 ep-text-color--black"}).string.replace("\n", "").strip()

    else:
        shoots = "-"

    if soup.find("div", {"class":"order-12 ep-list__item ep-list__item--in-card-body ep-list__item--is-compact"}) != None:
        draft = soup.find("div", {"class":"order-12 ep-list__item ep-list__item--in-card-body ep-list__item--is-compact"}
                         ).find(
            "div", {"class":"col-xs-12 col-18 text-right p-0"}).find("a").string.replace("\n", "").strip()
    else: 
        draft = "-"

    #height = np.where(height=="- / -", "-", height)

    #print(player + " scraped!")
    return(player, dob, height, weight, birthplace, nation, shoots, draft, link)
    
def get_player_information(dataframe):

    myplayer = []
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
            mydob.append(myresult[1])
            myheight.append(myresult[2])
            myweight.append(myresult[3])
            mybirthplace.append(myresult[4])
            mynation.append(myresult[5])
            myshot.append(myresult[6])
            mydraft.append(myresult[7])
            mylink.append(myresult[8])
            print(myresult[0] + " scraped! That's " + str(i + 1) + " down! Only " + str(len(list(set(dataframe.link))) - (i + 1)) +  " left to go!")
        except KeyboardInterrupt:
            print("You interrupted this one manually. The output here will be every player you've scraped so far. Good bye!")
            break
        except (ConnectionError,
            HTTPError,
            ReadTimeout,
            ConnectTimeout) as errormessage:
            print("Yo' ass is disconnected! Here's the error message:")
            print(errormessage)
            print("Luckily, everything you've scraped up to this point will still be safe.")
            break

    resultdf = pd.DataFrame(columns = ["player", "dob", "height", "weight", "birthplace", "nation", "shoots", "draft", "link"])

    resultdf.player = myplayer
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
        output = output.append(single)
        print("Scraping " + league + " data is complete. You scraped skater data from " + seasons + ".")
        return(output)
    
    elif ((type(seasons) == tuple) or (type(seasons) == list)):
    
        for i in range(0, len(seasons)):
            try:
                single = getskaters(league, seasons[i])
                output = output.append(single)
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
            
        print("Scraping " + league + " data is complete. You scraped skater data from " + scraped_season_list + ".")    
        return(output)
    
def get_league_goalie_boxcars(league, seasons):

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
        output = output.append(single)
        print("Scraping " + league + " data is complete. You scraped goalie data from " + seasons + ".")
        return(output)
    
    elif ((type(seasons) == tuple) or (type(seasons) == list)):
    
        for i in range(0, len(seasons)):
            try:
                single = getgoalies(league, seasons[i])
                output = output.append(single)
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
                leaguesall = leaguesall.append(targetleague)
                if hidden_patrick == 4:
                    raise KeyboardInterrupt
                if hidden_patrick == 5:
                    raise Patrick
            except KeyboardInterrupt:
                print("You interrupted this one manually. The output here will be every player you've scraped so far. Good bye!")
                break
            except Patrick:
                print("You were disconnected! The output here will be every player you've scraped so far. Here's your error message:")
                print(error)
                break
                
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
                leaguesall = leaguesall.append(targetleague)
                if hidden_patrick == 4:
                    raise KeyboardInterrupt
                if hidden_patrick == 5:
                    raise Patrick
            except KeyboardInterrupt:
                print("You interrupted this one manually. The output here will be every player you've scraped so far. Good bye!")
                break
            except Patrick:
                print("You were disconnected! The output here will be every player you've scraped so far. Here's your error message:")
                print(error)
                break
                
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
                leaguesall = leaguesall.append(targetleague)
                if hidden_patrick == 4:
                    raise KeyboardInterrupt
                if hidden_patrick == 5:
                    raise Patrick
            except KeyboardInterrupt:
                print("You interrupted this one manually. The output here will be every player you've scraped so far. Good bye!")
                break
            except Patrick:
                print("You were disconnected! The output here will be every player you've scraped so far. Here's your error message:")
                print(error)
                break
                
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
                leaguesall = leaguesall.append(targetleague)
                if hidden_patrick == 4:
                    raise KeyboardInterrupt
                if hidden_patrick == 5:
                    raise Patrick
            except KeyboardInterrupt:
                print("You interrupted this one manually. The output here will be every player you've scraped so far. Good bye!")
                break
            except Patrick:
                print("You were disconnected! The output here will be every player you've scraped so far. Here's your error message:")
                print(error)
                break
                
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
    with_player_info = get_player_information(dataframe)
    doubledup = dataframe.merge(with_player_info.drop(columns = ['player']), on = 'link', how = 'inner')
    return doubledup

### EXAMPLE ONE: GET ALL SKATERS FROM THE MHL IN 2020-2021 ###

mhl2021 = get_skaters("mhl", "2020-2021")

### NOW SORT THESE SKATERS BY POINTS PER GAME ("PPG") AND TAKE A LOOK AT THE TOP 20 ###

mhl2021.sort_values(by = 'ppg', ascending = False).head(20)

### EXAMPLE 2: GET DATA FOR MULTIPLE LEAGUES AND SEASONS ###

leagues = ("mhl", "khl")

seasons = ("2019-2020", "2020-2021")

mhl_khl_1921 = get_skaters(leagues, seasons)

### DISPLAY DATA FROM ONLY ONE OF THOSE LEAGUES USING LOC ###

mhl_khl_1921.loc[mhl_khl_1921.league=="khl"]

### EXAMPLE 3: ADD PLAYER INFORMATION DATA FOR THE TOP-20 MHL SCORERS WE LOOKED AT BEFORE, THEN TAKE A LOOK AT IT. ###

mhl_top_20 = mhl2021.sort_values(by = 'ppg', ascending = False).head(20)

mhl_top_20_with_info = add_player_information(mhl_top_20)

mhl_top_20_with_info

### EXAMPLE 4: LOOK AT SOME PRO GOALIES PLAYING IN AMERICA OVER THE PAST 3 YEARS ###

usaproleagues = ("ahl", "nhl")
last3years = ("2017-2018", "2018-2019", "2019-2020")

usaprogoalies = get_goalies(usaproleagues, last3years)