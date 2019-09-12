import requests
import xml.etree.ElementTree as ET
import time
import pandas as pd
import sql_functions  # other .py file
import api_key
import os


def get_xml(link, object_ids):
    """
        creates directory, XML files, and sends back path to XML file
    :param link: API link
    :param object_ids: gets season_id and team_id for file name
    :return: XML content / tree
    """

    if object_ids[1] is None:
        season_only = True
    else:
        season_only = False

    xml = requests.get(link)
    # print(xml.content)
    root = ET.fromstring(xml.text)
    tree = ET.ElementTree(root)

    try:
        # Create target Directory
        os.mkdir("Raw_XML_Files")
        os.mkdir("Raw_XML_Files/team")
        os.mkdir("Raw_XML_Files/season")
    except FileExistsError:
        pass

    if object_ids[0] is not None:
        if season_only:
            path = "Raw_XML_Files/season/"+str(object_ids[0])+".xml"
        else:
            path = "Raw_XML_Files/team/"+str(object_ids[0])+"_team_"+str(object_ids[1])+".xml"

    else:
        if season_only:
            path = "Raw_XML_Files/initial_season_setup.xml"
        else:
            path = "Raw_XML_Files/initial_team_setup.xml"

    tree.write(path)
    # print(path)

    return path


def remove_xsd(xml):
    """
        removes XSD from XML tags and returns tree
    :param xml: unclean XML
    :return root: cleaned XML
    """
    # from https://stackoverflow.com/a/25920989
    for _, el in xml:
        if '}' in el.tag:
            el.tag = el.tag.split('}', 1)[1]  # strip all namespaces
    root = xml.root
    return root


def create_obj_ids(testing, obj):
    """
        gets SportRadar's season and team IDs
    :param testing: if you are testing or debugging
    :param obj: team or season string
    :return: dictionary of SportRadar IDs (note: the IDs are not ints)
    """
    if testing:
        if obj == 'team':
            it = ET.iterparse("/Users/Atharva/Documents/Github/NBA_Analysis/teams-sample.xml")
        else:
            it = ET.iterparse("/Users/Atharva/Documents/Github/NBA_Analysis/seasons-sample.xml")
    else:
        if obj == 'team':
            response_body = get_xml('http://api.sportradar.us/nba/trial/v7/en/seasons/2018'
                                    '/REG/standings.xml?api_key=' + api_key.get_key(), [None, 1])
        else:
            response_body = get_xml('http://api.sportradar.us/nba/trial/v7/en/league/'
                                    'seasons.xml?api_key=' + api_key.get_key(), [None, None])

        print(str(response_body))
        it = ET.iterparse(str(response_body))

    root = remove_xsd(it)

    obj_dict = dict()

    skip_pre_season_index = 0

    for item in root.iter(obj):
        print(skip_pre_season_index)
        if skip_pre_season_index % 3 == 0 or skip_pre_season_index == 1:
            skip_pre_season_index += 1
            continue
        skip_pre_season_index += 1
        if obj == 'team':
            obj_dict[item.attrib['id']] = item.attrib['name']
        else:
            if skip_pre_season_index % 3 == 0 or skip_pre_season_index == 1:
                skip_pre_season_index += 1
                continue
            skip_pre_season_index += 1
            if item.attrib['year'] == '2012': # or item.attrib['type'] == 'PRE':
                continue
            obj_dict[item.attrib['id']] = item.attrib['year']

    print(obj_dict)
    return obj_dict


def get_parent_entity_id(feed, root):
    """
        want to get the parent entity IDs. In this case, the Team's parent entity is the season. For player, team.
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
            return team_id


def df_generator(testing, tree, feeds, season_and_team_ids, file_type):
    """
        this is the most confusing part. There are a lot of small things going on here.

    First we make lists for the keys and values. I didn't use a dictionary because the
    values will be updated every loop in the inner loop. Since this loop is shared by the team DF generations as well as
    the players DF, this uses generic terms. It goes through the process and generates 3 DFs that will feed the insert
    statements into the DB.

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

    root = remove_xsd(tree)

    # this will get the parent entity ID so we can have put it in the DB as a FK. Team's parent = Season, player's
    # parent = Team.
    parent_entity_id = get_parent_entity_id(feeds[0], root)

    attrib_entity_values = list()
    attrib_entity_keys = list()
    attrib_total_values = list()
    attrib_total_keys = list()
    attrib_average_values = list()
    attrib_average_keys = list()

    # this line is here because the tree has <team>, and children <player_records> and <team_records>. Using base_node
    # for the first loop ensures that we don't take the player_records "total"/"avg" attributes (that is how the iter()
    # function from ElementTree works). Splitting it allows us to use team (which we need for the team entity list,
    # and team_records to limit it to ONLY team totals and averages. This is only used for team_records
    base_node = feeds[0].split('_')[0]

    for item in root.iter(base_node):
        if len(item.attrib.keys()) > len(attrib_entity_keys):
            attrib_entity_keys = (list(item.attrib.keys()))

    print(attrib_entity_keys)

    # fills the entity keys and values
    for item in root.iter(base_node):

        # 9/9/2019 - apparently some players don't have a jersey number in the XML provided. this if statement inserts
        # a null instead of whatever the next value would have been
        if item.tag == 'player':
            if "jersey_number" not in item.attrib:
                jersey_index = attrib_entity_keys.index('jersey_number')

                xml_attrib = list(item.attrib.values())
                xml_attrib.insert(jersey_index, 'None')
                attrib_entity_values.append(xml_attrib)
                continue

        # this adds the current total records to the list, where each total.attrib.values() is a new list item
        attrib_entity_values.append(list(item.attrib.values()))



    # obj would be "team_records" or "player", this will limit it to the correct node children
    for obj in root.iter(feeds[0]):
        # print(obj.attrib)

        for tot in obj.iter(feeds[1]):
            if len(tot.attrib.keys()) > len(attrib_total_keys):
                attrib_total_keys = (list(tot.attrib.keys()))
        for avg in obj.iter(feeds[2]):
            if len(avg.attrib.keys()) > len(attrib_average_keys):
                attrib_average_keys = (list(avg.attrib.keys()))

        # Within the _records nodes, we pull the total and average for each entity.
        # this logic is the same as the loop above, just that it is using feeds as the filter in iter()
        for total in obj.iter(feeds[1]):
            if file_type == 'stats' and feeds[0] == 'player':
                if 'id' not in attrib_total_keys:
                    attrib_total_keys.append('id')

                total_values_with_ids = list(total.attrib.values())
                total_values_with_ids.append(str(obj.attrib['id']))
                attrib_total_values.append(total_values_with_ids)
                continue

            # if len(attrib_total_keys) == 0:
            #     attrib_total_keys = (list(total.attrib.keys()))
            attrib_total_values.append(list(total.attrib.values()))

        for average in obj.iter(feeds[2]):
            if file_type == 'stats' and feeds[0] == 'player':
                if 'id' not in attrib_average_keys:
                    attrib_average_keys.append('id')

                average_values_with_ids = list(average.attrib.values())
                average_values_with_ids.append(str(obj.attrib['id']))
                attrib_average_values.append(average_values_with_ids)
                continue

            attrib_average_values.append(list(average.attrib.values()))

    print(attrib_total_keys)
    print(attrib_total_values)

    # convert the list of lists of values, and keys into a DF, feed this to the insert functions
    entity_df = pd.DataFrame(attrib_entity_values, columns=attrib_entity_keys)
    total_df = pd.DataFrame(attrib_total_values, columns=attrib_total_keys)
    average_df = pd.DataFrame(attrib_average_values, columns=attrib_average_keys)

    print('end of function')

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
        season_id = get_parent_entity_id('team', root)  # need this so that we can fill season_id on the Player table
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
    time.sleep(1)
    team_ids = create_obj_ids(testing, 'team')

    team_feeds = ['team_records', 'total', 'average']
    player_feeds = ['player', 'total', 'average']

    sql_functions.create_db(testing)
    sql_functions.populate_seasons_table(testing, season_ids)

    if testing:
        it = ET.iterparse("/Users/Atharva/Documents/Github/NBA_Analysis/teams-sample.xml")

        season_id = '47c9979e-5c3f-453d-ac75-734d17412e3f'
        team_id = '583eca2f-fb46-11e1-82cb-f4ce4684ea4c'
        season_and_team_ids = [season_id, team_id]

        # it is working up til here.
        df_generator(testing, it, team_feeds, None, 'teams')

        it = ET.iterparse("/Users/Atharva/Documents/Github/NBA_Analysis/nba-team-season-stats-sample.xml")
        df_generator(testing, it, team_feeds, season_and_team_ids, 'stats')
        df_generator(testing, it, player_feeds, season_and_team_ids, 'stats')
    else:
        ind = 0
        for season_id in season_ids:

            list_season_ids = list(season_ids)

            index_mod = (list_season_ids.index(season_id)) % 3

            if index_mod == 0 or index_mod == 1:
                continue
            else:
                season_type = 'REG'

            # teams
            time.sleep(1)  # API allows 1 call/second
            api_call = get_xml('http://api.sportradar.us/nba/trial/v7/en/seasons/' + str(season_ids[season_id]) +
                               '/' + season_type + '/standings.xml?api_key=' + api_key.get_key(), [season_id, None])

            it = ET.iterparse(api_call)
            df_generator(testing, it, team_feeds, None, 'teams')

            ind2 = 0
            for team in team_ids:
                season_and_team_ids = [season_id, team]
                # print(season_and_team_ids)

                # all player info, TeamTotal, TeamAverage
                time.sleep(1)  # API allows 1 call/second

                api_call = get_xml('http://api.sportradar.us/nba/trial/v7/en/seasons/' + str(season_ids[season_id]) +
                                   '/' + season_type + '/teams/' + team + '/statistics.xml?api_key=' +
                                   api_key.get_key(), season_and_team_ids)

                it = ET.iterparse(api_call)
                df_generator(testing, it, team_feeds, season_and_team_ids, 'stats')
                df_generator(testing, it, player_feeds, season_and_team_ids, 'stats')

                #print('api call:', api_call)


                #print(ind2)
                ind2 += 1
                if ind2 == 10:
                    break

            #print(season_id)

            print(ind)
            ind += 1
            if ind == 6:
                break

    sql_functions.clean_up_missing_jerseys(testing)


testing_input = input('testing? Anything other than "Not Testing" will be treated as Yes.\n')

if testing_input == 'Not Testing':
    testing = False
else:
    testing = True

compile_stats(testing)
