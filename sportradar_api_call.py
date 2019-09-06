import requests
import xml.etree.ElementTree as ET
import time
import sqlite3
import pandas as pd
# first need team IDs - seems like we have to pull this from their Standings API call
# http://api.sportradar.us/nba/trial/v7/en/seasons/2018/REG/standings.xml?api_key=f795md7eb6e8thbecwchmud8


# creates database with tables according to the FieldAnalysis.xlsx > Proposed tables
def create_db(testing):
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS Season (
        [id] INTEGER PRIMARY KEY,
        [season_name] text, 
        [season_id] integer, 
        [sportradar_id] text,
        [type] text,
        [add_date] date,
        [update_date] date,
        [version] integer,
        [is_deleted] boolean
        )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Team (
        [id] INTEGER PRIMARY KEY,
        [name] text,
        [market] text,
        [year] integer
        [season_id] text,
        [sr_id] text
        [sr_internal] text,
        [reference] int,
        [add_date] date,
        [update_date] date,
        [version] integer,
        [is_deleted] boolean
        )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS TeamTotal (
        [id] INTEGER PRIMARY KEY,
        [team_id] integer,
        [games_played] integer,
        [minutes] float,
        [field_goals_made] integer,
        [field_goals_att] integer,
        [field_goals_pct] float,
        [two_points_made] integer,
        [two_points_att] integer,
        [two_points_pct] float,
        [three_points_made] integer,
        [three_points_att] integer,
        [three_points_pct] float,
        [blocked_att] integer,
        [free_throws_made] integer,
        [free_throws_att] integer,
        [free_throws_pct] float,
        [offensive_rebounds] integer,
        [defensive_rebounds] integer,
        [rebounds] integer,
        [assists] integer,
        [turnovers] integer,
        [assists_turnover_ratio] float,
        [steals] integer,
        [blocks] integer,
        [personal_fouls] integer,
        [tech_fouls] integer,
        [points] integer,
        [fast_break_pts] integer,
        [flagrant_fouls] integer,
        [points_off_turnovers] integer,
        [second_chance_pts] integer,
        [ejections] integer,
        [foulouts] integer,
        [points_in_paint] integer,
        [efficiency] integer,
        [true_shooting_att] integer,
        [true_shooting_pct] float,
        [points_in_paint_made] integer,
        [points_in_paint_att] integer,
        [points_in_paint_pct] float,
        [effective_fg_pct] float,
        [bench_points] integer,
        [fouls_drawn] integer,
        [offensive_fouls] integer,
        [team_tech_fouls] integer,
        [defensive_assists] integer,
        [fast_break_att] integer,
        [fast_break_made] integer,
        [fast_break_pct] float,
        [technical_other] integer,
        [coach_ejections] integer,
        [points_against] integer,
        [team_defensive_rebounds] integer,
        [team_offensive_rebounds] integer,
        [second_chance_att] integer,
        [second_chance_made] integer,
        [second_chance_pct] float,
        [coach_tech_fouls] integer,
        [team_fouls] integer,
        [total_rebounds] integer,
        [total_fouls] integer,
        [add_date] date,
        [update_date] date,
        [version] integer,
        [is_deleted] boolean
        )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS TeamAverage (
        [id] INTEGER PRIMARY KEY,
        [team_id] integer,
        [is_opponent] integer,
        [fast_break_pts] float,
        [points_off_turnovers] float,
        [second_chance_pts] float,
        [minutes] float,
        [points] float,
        [off_rebounds] float,
        [def_rebounds] float,
        [rebounds] float,
        [assists] float,
        [steals] float,
        [blocks] float,
        [turnovers] float,
        [personal_fouls] float,
        [flagrant_fouls] float,
        [blocked_att] float,
        [field_goals_made] float,
        [field_goals_att] float,
        [three_points_made] float,
        [three_points_att] float,
        [free_throws_made] float,
        [free_throws_att] float,
        [two_points_made] float,
        [two_points_att] float,
        [points_in_paint] float,
        [efficiency] float,
        [true_shooting_att] float,
        [points_in_paint_att] float,
        [points_in_paint_made] float,
        [bench_points] float,
        [fouls_drawn] float,
        [offensive_fouls] float,
        [fast_break_att] float,
        [fast_break_made] float,
        [second_chance_att] float,
        [second_chance_made] float,
        [add_date] date,
        [update_date] date,
        [version] integer,
        [is_deleted] boolean
        )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Player (
        [id] INTEGER PRIMARY KEY,
        [team_id] integer,
        [season_id] integer,
        [first_name] text,
        [last_name] text,
        [year] integer,
        [sr_id] text,
        [sr_internal] text,
        [Reference] integer,
        [position] text,
        [primary_position] text,
        [jersey_number] integer,
        [add_date] date,
        [update_date] date,
        [version] integer,
        [is_deleted] boolean
        )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS PlayerTotal (
        [id] INTEGER PRIMARY KEY,
        [player_id] integer,
        [games_played] integer,
        [games_started] integer,
        [minutes] integer,
        [field_goals_made] integer,
        [field_goals_att] integer,
        [field_goals_pct] float,
        [two_points_made] integer,
        [two_points_att] integer,
        [two_points_pct] float,
        [three_points_made] integer,
        [three_points_att] integer,
        [three_points_pct] float,
        [blocked_att] integer,
        [free_throws_made] integer,
        [free_throws_att] integer,
        [free_throws_pct] float,
        [offensive_rebounds] integer,
        [defensive_rebounds] integer,
        [rebounds] integer,
        [assists] integer,
        [turnovers] integer,
        [assists_turnover_ratio] float,
        [steals] integer,
        [blocks] integer,
        [personal_fouls] integer,
        [tech_fouls] integer,
        [points] integer,
        [flagrant_fouls] integer,
        [ejections] integer,
        [foulouts] integer,
        [true_shooting_att] integer,
        [true_shooting_pct] float,
        [efficiency] integer,
        [points_off_turnovers] integer,
        [points_in_paint] integer,
        [points_in_paint_made] integer,
        [points_in_paint_att] integer,
        [points_in_paint_pct] float,
        [effective_fg_pct] float,
        [double_doubles] integer,
        [triple_doubles] integer,
        [fouls_drawn] integer,
        [offensive_fouls] integer,
        [fast_break_pts] integer,
        [fast_break_att] integer,
        [fast_break_made] integer,
        [fast_break_pct] float,
        [coach_ejections] integer,
        [second_chance_pct] float,
        [second_chance_pts] integer,
        [second_chance_att] integer,
        [second_chance_made] integer,
        [minus] integer,
        [plus] integer,
        [coach_tech_fouls] integer,
        [add_date] date,
        [update_date] date,
        [version] integer,
        [is_deleted] boolean
        )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS PlayerAverage (
        [id] INTEGER PRIMARY KEY,
        [player_id] integer,
        [minutes] float,
        [points] float,
        [off_rebounds] float,
        [def_rebounds] float,
        [rebounds] float,
        [assists] float,
        [steals] float,
        [blocks] float,
        [turnovers] float,
        [personal_fouls] float,
        [flagrant_fouls] float,
        [blocked_att] float,
        [field_goals_made] float,
        [field_goals_att] float,
        [three_points_made] float,
        [three_points_att] float,
        [free_throws_made] float,
        [free_throws_att] float,
        [two_points_made] float,
        [two_points_att] float,
        [efficiency] float,
        [true_shooting_att] float,
        [points_off_turnovers] float,
        [points_in_paint_made] float,
        [points_in_paint_att] float,
        [points_in_paint] float,
        [fouls_drawn] float,
        [offensive_fouls] float,
        [fast_break_pts] float,
        [fast_break_att] float,
        [fast_break_made] float,
        [second_chance_pts] float,
        [second_chance_att] float,
        [second_chance_made] float,
        [add_date] date,
        [update_date] date,
        [version] integer,
        [is_deleted] boolean
        )''')
    conn.commit()

##############################################################################################################################should do this one first, the rest can mimic this
def populate_seasons_table(testing, seasons, season_ids):
    """
        Inserts season records into the season tables
    :param testing: if you are testing or debugging
    :param seasons: list of years of seasons we are getting data for
    :param season_ids: sport radar IDs for each respective season
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')
    cur = conn.cursor()
    conn.commit()


def get_xml(link):
    """
        gets the XML tree
    :param link: API link
    :return: XML content / tree
    """
    xml = requests.get(link)
    return xml.content


# take the root to parse and get the ID
def create_obj_ids(testing, obj):
    """
        gets SportRadar's season and team IDs
    :param testing: if you are testing or debugging
    :param obj: team or season string
    :return: list of SportRadar IDs (not ints)
    """
    if testing:
        if obj == 'team':
            it = ET.iterparse("/Users/Atharva/Documents/Github/NBA_Analysis/teams-sample.xml")
        else:
            it = ET.iterparse("/Users/Atharva/Documents/Github/NBA_Analysis/seasons-sample.xml")
    else:
        if obj == 'team':
            response_body = get_xml('http://api.sportradar.us/nba/trial/v7/en/seasons/2018/REG/standings.xml?api_key=f795md7eb6e8thbecwchmud8')
        else:
            response_body = get_xml('http://api.sportradar.us/nba/trial/v7/en/league/seasons.xml?api_key=f795md7eb6e8thbecwchmud8')
        it = ET.iterparse(response_body)

    # from https://stackoverflow.com/a/25920989
    for _, el in it:
        if '}' in el.tag:
            el.tag = el.tag.split('}', 1)[1]  # strip all namespaces
    root = it.root

    object_id_list = list()

    for item in root.iter(obj):

        list_id = item.attrib['id']  # returns a dictionary with each attrib of the team tag, pulling only ID
        object_id_list.append(list_id)

    print(obj)
    print(object_id_list)
    return object_id_list


def insert_team_team(testing, df):
    """
        insert team data into DB
    :param testing: if you are testing or debugging
    :param df: data frame generated to place into DB
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    print('1\n', df)
    cur = conn.cursor()
    conn.commit()
    pass


def insert_team_total(testing, df):
    """
        insert team totals data into DB
    :param testing: if you are testing or debugging
    :param df: data frame generated to place into DB
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    print('2\n', df)
    cur = conn.cursor()
    conn.commit()
    pass


def insert_team_average(testing, df):
    """
        insert team averages data into DB
    :param testing: if you are testing or debugging
    :param df: data frame generated to place into DB
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    print('3\n', df)
    cur = conn.cursor()
    conn.commit()
    pass


def insert_player_player(testing, df):
    """
        insert player data into DB
    :param testing: if you are testing or debugging
    :param df: data frame generated to place into DB
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    print('4\n', df)
    cur = conn.cursor()
    conn.commit()
    pass


def insert_player_total(testing, df):
    """
        insert player totals data into DB
    :param testing: if you are testing or debugging
    :param df: data frame generated to place into DB
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    print('5\n', df)
    cur = conn.cursor()
    conn.commit()
    pass


def insert_player_average(testing, df):
    """
        insert player averages data into DB
    :param testing: if you are testing or debugging
    :param df: data frame generated to place into DB
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    print('6\n', df)
    cur = conn.cursor()
    conn.commit()
    pass


def df_generator(testing, tree, feeds):
    """
        this is the most confusing part. There are a lot of small things going on here.

    First we clean the XML as needed. Then, we make lists for the keys and values. I didn't use a dictionary because the
    values will be updated every loop in the inner loop. Since this loop is shared by the team DF generations as well as
    the players DF, this uses generic terms which certainly don't help in understanding. It goes through the process and
    generates 3 DFs that will feed the insert statements into the DB.

    :param testing: if you are testing or debugging
    :param tree: the entire XML tree provided by the API
    :param feeds: this is a 3-item string, with either "player_records" or "team_records" in the first slot, then
    "total" and "average"
    :return:

    """
    # from https://stackoverflow.com/a/25920989
    for _, el in tree:
        if '}' in el.tag:
            el.tag = el.tag.split('}', 1)[1]  # strip all namespaces
    root = tree.root

    attrib_entity_values = list()
    attrib_entity_keys = list()
    attrib_total_values = list()
    attrib_total_keys = list()
    attrib_avg_values = list()
    attrib_avg_keys = list()

    # this line is here because the tree has <team>, and children <player_records> and <team_records>. Using base_node
    # for the first loop ensures that we don't take the player_records "total"/"avg" attributes (that is how the iter()
    # function from ElementTree works). Splitting it allows us to use team (which we need for the team entity list,
    # and team_records to limit it to ONLY team totals and averages
    #
    # Even worse, for players, <player_records> is the parent to <player>. But this handles that because it shouldn't
    # player_records doesn't have it's own attributes.
    base_node = feeds[0].split('_')[0]

    # So we import team_record but are only using team for the team entity attributes (this loop)
    for item in root.iter(base_node):
        # don't have to do this for keys every time, is only for the DF headers
        if len(attrib_entity_keys) == 0:
            attrib_entity_keys = (list(item.attrib.keys()))
        # this adds the current total records to the list, where each total.attrib.values() is a new list item
        attrib_entity_values.append(list(item.attrib.values()))

    # obj would be "team_records" or "player_records", this will limit it to the correct node children
    for obj in root.iter(feeds[0]):
        # Within the _records nodes, we pull the total and average for each entity.
        # this logic is the same as the loop above, just that it is using feeds as the filter in iter()
        for total in obj.iter(feeds[1]):
            if len(attrib_total_keys) == 0:
                attrib_total_keys = (list(total.attrib.keys()))
            attrib_total_values.append(list(total.attrib.values()))

        for average in obj.iter(feeds[2]):
            if len(attrib_avg_keys) == 0:
                attrib_avg_keys = (list(average.attrib.keys()))
            attrib_avg_values.append(list(average.attrib.values()))

    # convert the list of lists of values, and keys into a DF, feed this to the insert functions
    entity_df = pd.DataFrame(attrib_entity_values, columns=attrib_entity_keys)
    total_df = pd.DataFrame(attrib_total_values, columns=attrib_total_keys)
    average_df = pd.DataFrame(attrib_avg_values, columns=attrib_avg_keys)

    # call certain products based on the entity
    if feeds[0] == 'team_records':
        insert_team_team(testing, entity_df)
        insert_team_total(testing, total_df)
        insert_team_average(testing, average_df)
    else:
        insert_player_player(testing, entity_df)
        insert_player_total(testing, total_df)
        insert_player_average(testing, average_df)


def compile_stats(testing):
    """
        runs the program. will call the API and fill in collected data into a DB
        API call:
        http://api.sportradar.us/nba/trial/v7/en/seasons/2018/REG/teams/583eca2f-fb46-11e1-82cb-f4ce4684ea4c
        /statistics.xml?api_key=f795md7eb6e8thbecwchmud8
    :param testing: if you are testing or debugging
    :return:
    """
    season_ids = create_obj_ids(testing, 'season')
    team_ids = create_obj_ids(testing, 'team')
    seasons = [2013, 2014, 2015, 2016, 2016, 2017, 2018, 2019]

    create_db(testing)
    populate_seasons_table(testing, seasons, season_ids)

    if testing:
        it = ET.iterparse("/Users/Atharva/Documents/Github/NBA_Analysis/nba-team-season-stats-sample.xml")
        df_generator(testing, it, ['team_records', 'total', 'average'])
        df_generator(testing, it, ['player_records', 'total', 'average'])
    else:
        for season in seasons:
            for team in team_ids:
                time.sleep(1) # API allows 1 call/second
                response_xml = 'http://api.sportradar.us/nba/trial/v7/en/seasons/' + season + '/REG/teams/' + team + '/statistics.xml?api_key=f795md7eb6e8thbecwchmud8'
                content = response_xml.content
                it = ET.iterparse(content)
                nothing_happening = creator(testing, it, feeds, )


compile_stats(True)