import requests
import xml.etree.ElementTree as ET
import time
import sqlite3
import pandas as pd
import sql_functions  # other .py file


def get_xml(link):
    """
        gets the XML tree
    :param link: API link
    :return: XML content / tree
    """
    xml = requests.get(link)
    return xml.content


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
        time.sleep(1)  # 1 api call per second
        if obj == 'team':
            response_body = get_xml('http://api.sportradar.us/nba/trial/v7/en/seasons/2018'
                                    '/REG/standings.xml?api_key=f795md7eb6e8thbecwchmud8')
        else:
            response_body = get_xml('http://api.sportradar.us/nba/trial/v7/en/league/'
                                    'seasons.xml?api_key=f795md7eb6e8thbecwchmud8')
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

    return object_id_list


def get_parent_entity_id(feed, root):
    """
    want to get the parent entity IDs. In this case, the Team's parent entity is the season. For player, it's team.
    :param feed: this will be 'player' or 'team' to decide which parent we need to get
    :param root: this is the XML tree
    :return: return the season or team ID based on the feed
    """
    if 'team' in feed:
        for item in root.iter('season'):
            season_id = item.attrib['id']
            return season_id

    else:
        for item in root.iter('team'):
            team_id = item.attrib['id']
            # print("THIS IS THE TEAM ID:", team_id)
            return team_id


def df_generator(testing, tree, feeds, season_and_team_ids, file_type):
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
    :param team_id: on the first call, it is returned and used to link the TeamTotal and TeamAverage tables to the
    teamID
    :param file_type: this is just a filter to not have the team's initially generated again. Causes error if removed
    because the files that we are using are different.
    :return:
    """
    # from https://stackoverflow.com/a/25920989
    for _, el in tree:
        if '}' in el.tag:
            el.tag = el.tag.split('}', 1)[1]  # strip all namespaces
    root = tree.root

    # this will get the parent entity ID so we can have put it in the DB as a FK. Team's parent = Season, player's
    # parent = Team. Note that it is only used for the Team and Player inserts, not all of them.
    parent_entity_id = get_parent_entity_id(feeds[0], root)

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

    jersey_index = -1

    # So we import team_record but are only using team for the team entity attributes (this loop)
    for item in root.iter(base_node):
        no_jersey_for_player = False

        if item.tag == 'player':
            if "jersey_number" not in item.attrib:
                no_jersey_for_player = True

        if len(attrib_entity_keys) == 0:
            attrib_entity_keys = (list(item.attrib.keys()))

            if item.tag == 'player':
                jersey_index = attrib_entity_keys.index('jersey_number')

        # this adds the current total records to the list, where each total.attrib.values() is a new list item
        # 9/9/2019 - apparently some players don't have a jersey number in the XML provided. this if statement inserts
        # a null instead of whatever the next value would have been
        if no_jersey_for_player:
            xml_attribs = list(item.attrib.values())
            xml_attribs.insert(jersey_index, 'None')
            attrib_entity_values.append(xml_attribs)
            continue  # skip the other append

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

    # print(attrib_total_values)

    # convert the list of lists of values, and keys into a DF, feed this to the insert functions
    entity_df = pd.DataFrame(attrib_entity_values, columns=attrib_entity_keys)
    total_df = pd.DataFrame(attrib_total_values, columns=attrib_total_keys)
    average_df = pd.DataFrame(attrib_avg_values, columns=attrib_avg_keys)

    # call certain products based on the entity
    if feeds[0] == 'team_records':
        # need this so that we can fill team_id in the Total / Average tables
        if file_type == 'teams':
            sql_functions.insert_team(testing, entity_df, parent_entity_id)

            for index, row in entity_df.iterrows():
                season_and_team_ids = [parent_entity_id, row['id']]
                break
        else:
            sql_functions.insert_team_total(testing, total_df, season_and_team_ids)
            sql_functions.insert_team_average(testing, average_df, season_and_team_ids)

        return season_and_team_ids
    else:
        season_id = get_parent_entity_id('team', root) # need this so that we can fill season_id on the Player table
        sql_functions.insert_player(testing, entity_df, parent_entity_id, season_id, total_df, average_df)


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
    seasons = [2012, 2013, 2014, 2015, 2016, 2016, 2017, 2018, 2019]

    team_feeds = ['team_records', 'total', 'average']
    player_feeds = ['player_records', 'total', 'average']

    sql_functions.create_db(testing)
    sql_functions.populate_seasons_table(testing, seasons, season_ids)

    if testing:
        it = ET.iterparse("/Users/Atharva/Documents/Github/NBA_Analysis/teams-sample.xml")
        season_id = '47c9979e-5c3f-453d-ac75-734d17412e3f'
        team_id = '583eca2f-fb46-11e1-82cb-f4ce4684ea4c'

        season_and_team_ids = [season_id, team_id]

        df_generator(testing, it, team_feeds, None, 'teams')

        # it is working up til here.
        it = ET.iterparse("/Users/Atharva/Documents/Github/NBA_Analysis/nba-team-season-stats-sample.xml")
        df_generator(testing, it, team_feeds, season_and_team_ids, 'stats')
        df_generator(testing, it, player_feeds, season_and_team_ids, 'stats')
    else:
        for season in seasons:
            # teams
            time.sleep(1)  # API allows 1 call/second
            response_xml = 'http://api.sportradar.us/nba/trial/v7/en/seasons/' + str(season) + \
                           '/REG/standings.xml?api_key=f795md7eb6e8thbecwchmud8'
            content = response_xml.content
            it = ET.iterparse(content)
            df_generator(testing, it, team_feeds, None, 'teams')

            for team in team_ids:

                season_and_team_ids = [season, team]  ###### this is WRONG! NEEDS TO USE THE SEASON_IDS list !!!!!!!!!!!!!

                # players
                time.sleep(1)  # API allows 1 call/second
                response_xml = 'http://api.sportradar.us/nba/trial/v7/en/seasons/' + str(season) + '/REG/teams/' \
                               + team + '/statistics.xml?api_key=f795md7eb6e8thbecwchmud8'
                content = response_xml.content
                it = ET.iterparse(content)
                df_generator(testing, it, team_feeds, season_and_team_ids, 'stats')
                df_generator(testing, it, player_feeds, season_and_team_ids, 'stats')


compile_stats(True)