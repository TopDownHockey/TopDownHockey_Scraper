# TopDownHockey EliteProspects Scraper

## By Patrick Bacon, made possible by the work of Marcus Sjölin and Harry Shomer.

---

This is a package built for scraping two data sources:

1. The NHL's Play-by-Play Reports, which come in the form of HTML/API reports from the NHL and JSON reports from ESPN.

2. Elite Prospects, an extremely valuable website which makes hockey data for thousands of leagues available to the public. 

This package is strictly built for end users who wish to scrape data for personal use. If you are interested in using Elite Prospects data for professional purposes, I recommend you look into the <a href="https://www.eliteprospects.com/api" >Elite Prospects API</a>.

While using the scraper, please be mindful of EliteProspects servers.

# Installation

---

You can install the package by entering the following command in terminal:

<code>pip install TopDownHockey_Scraper</code>

If you're interested in using the NHL Play-By-Play scraper, import that module using this function in Python:

<code>import TopDownHockey_Scraper.TopDownHockey_NHL_Scraper as tdhnhlscrape</code>

If you're interested in using the Elite Prospects scraper, import that module using this function in Python:

<code>import TopDownHockey_Scraper.TopDownHockey_EliteProspects_Scraper as tdhepscrape</code>

# User-End Functions (NHL Scraper)

---

### scrape_full_schedule(start_date, end_date)

Returns the NHL's schedule from the API for all games for the 2023-2024 NHL season.
    
Example:

<code>tdhnhlscrape.scrape_full_schedule()</code>

---

### full_scrape(game_id_list, shift = True)

Returns a dataframe containing play-by-play data for a list of game ids.

<ul>
    <li>game_id_list: A list of NHL game ids.</li>
    </ul>
    
Example: 

<code>tdhnhlscrape.full_scrape([2023020179, 2023020180, 2023020181])</code>
 

# User-End Functions (Elite Prospects Scraper)

---

### get_skaters(leagues, seasons)

Returns a dataframe containing statistics for all skaters in a target set of league(s) and season(s). 

<ul>
    <li>leagues: One or multiple leagues. If one league, enter as a string i.e; "nhl". If multiple leagues, enter as a tuple or list i.e; ("nhl", "ahl").</li>
    <li>seasons: One or multiple leagues. If one league, enter as a string i.e; "2018-2019". If multiple leagues, enter as a tuple or list i.e; ("2018-2019", "2019-2020").</li>
    </ul>

Example:

<code>tdhepscrape.get_skaters(("nhl", "ahl"), ("2018-2019", "2019-2020"))</code>

---

### get_goalies(leagues, seasons)

Returns a dataframe containing statistics for all goalies in a target set of league(s) and season(s). 

<ul>
    <li>leagues: One or multiple leagues. If one league, enter as a string i.e; "nhl". If multiple leagues, enter as a tuple or list i.e; ("nhl", "ahl").</li>
    <li>seasons: One or multiple leagues. If one league, enter as a string i.e; "2018-2019". If multiple leagues, enter as a tuple or list i.e; ("2018-2019", "2019-2020").</li>
    </ul>

Example:

<code>tdhepscrape.get_goalies("khl", "2015-2016")</code>

---
    
### get_player_information(dataframe)

Returns a dataframe containing bio information for all skaters or goalies (or both) within a target dataframe. 

<ul>
    <li>dataframe: The dataframe returned by one of the previous two commands.</li>
    </ul>

Example:

Say you obtain skater data for the KHL in 2020-2021 and store that as a dataframe called <code>output</code>. You can run this function to get bio information for every player in that league's scrape.

<code>output = tdhepscrape.get_skaters("khl", "2020-2021")</code>

<code>tdhepscrape.get_player_information(output)</code>

---

### add_player_information(dataframe)

Returns a dataframe containing bio information for all skaters or goalies (or both) within a target dataframe as well as the statistics from the original dataframe. 

<ul>
    <li>dataframe: The dataframe returned by one of the previous two commands.</li>
    </ul>

Example:

Say you obtain skater data for the KHL in 2020-2021 and store that as a dataframe called <code>output</code>. You can run this function to get bio information for every player in that league's scrape.

<code>output = tdhepscrape.get_skaters("khl", "2020-2021")</code>

<code>tdhepscrape.add_player_information(output)</code>

# Comments, Questions, and Concerns.

---

My goal was to make this package as error-proof as possible. I believe I've accounted for every issue that could potentially throw off a scrape, but it's possible I've missed something.

If any issues arise, or you have any questions about the package, please do not hesitate to contact me on Twitter at @TopDownHockey or email me directly at patrick.s.bacon@gmail.com.  