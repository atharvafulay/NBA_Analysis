import requests
import xml.etree.ElementTree as ET
import time
import sqlite3
import pandas as pd


def create_db(testing):
    """
        creates the DB (if it doesn't exist) according to the FieldAnalysis.xlsx > Proposed tables
    :param testing: if you are testing or debugging
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS Season (
        [id] INTEGER PRIMARY KEY,
        [name] text NOT NULL, 
        [codename] text NOT NULL, 
        [year] integer NOT NULL, 
        [sr_id] text NOT NULL, 
        [type] text,
        [add_date] date NOT NULL, 
        [update_date] date NOT NULL, 
        [version] integer NOT NULL, 
        [is_deleted] boolean,
        CONSTRAINT unique_codename UNIQUE (codename)
        );''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Team (
        [id] INTEGER PRIMARY KEY,
        [season_id] text,
        [name] text,
        [market] text,
        [wins] integer, 
        [losses] integer, 
        [win_pct] float, 
        [points_for] float,
        [points_against] float, 
        [point_diff] float,
        [sr_id] text,
        [sr_internal] text,
        [reference] int,
        [add_date] date,
        [update_date] date,
        [version] integer,
        [is_deleted] boolean,
        CONSTRAINT unique_codename UNIQUE (season_id, sr_id),
        CONSTRAINT season_id_FK FOREIGN KEY (season_id) REFERENCES Season(sr_id)
        );''')

    cur.execute('''CREATE TABLE IF NOT EXISTS TeamTotal (
        [id] INTEGER PRIMARY KEY,
        [team_id] text,
        [season_id] text,
        [games_played] integer,
        [is_opponent] boolean,
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
        [is_deleted] boolean,
        CONSTRAINT unique_cluster UNIQUE (team_id, season_id, is_opponent),
        CONSTRAINT team_id_FK FOREIGN KEY (team_id) REFERENCES Team(sr_id),
        CONSTRAINT season_id_FK FOREIGN KEY (season_id) REFERENCES Season(sr_id)
        )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS TeamAverage (
        [id] INTEGER PRIMARY KEY,
        [team_id] text,
        [season_id] text,
        [is_opponent] boolean,
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
        [is_deleted] boolean,
        CONSTRAINT unique_cluster UNIQUE (team_id, season_id, is_opponent),
        CONSTRAINT team_id_FK FOREIGN KEY (team_id) REFERENCES Team(sr_id),
        CONSTRAINT season_id_FK FOREIGN KEY (season_id) REFERENCES Season(sr_id)
        )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Player (
        [id] INTEGER PRIMARY KEY,
        [team_id] text,
        [season_id] text,
        [first_name] text,
        [last_name] text,
        [sr_id] text,
        [sr_internal] text,
        [Reference] integer,
        [position] text,
        [primary_position] text,
        [jersey_number] text,
        [add_date] date,
        [update_date] date,
        [version] integer,
        [is_deleted] boolean,
        CONSTRAINT unique_cluster UNIQUE (team_id, season_id, sr_id),
        CONSTRAINT team_id_FK FOREIGN KEY (team_id) REFERENCES Team(sr_id),
        CONSTRAINT season_id_FK FOREIGN KEY (season_id) REFERENCES Season(sr_id)
        )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS PlayerTotal (
        [id] INTEGER PRIMARY KEY,
        [player_id] text,
        [team_id] text,
        [season_id] text,
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
        [is_deleted] boolean,
        CONSTRAINT unique_cluster UNIQUE (player_id, team_id, season_id),
        CONSTRAINT player_id_FK FOREIGN KEY (player_id) REFERENCES Player(sr_id),
        CONSTRAINT team_id_FK FOREIGN KEY (team_id) REFERENCES Team(sr_id),
        CONSTRAINT season_id_FK FOREIGN KEY (season_id) REFERENCES Season(sr_id)
        )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS PlayerAverage (
        [id] INTEGER PRIMARY KEY,
        [player_id] integer,
        [team_id] text,
        [season_id] text,
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
        [is_deleted] boolean,
        CONSTRAINT unique_cluster UNIQUE (player_id, team_id, season_id),
        CONSTRAINT player_id_FK FOREIGN KEY (player_id) REFERENCES Player(sr_id),
        CONSTRAINT team_id_FK FOREIGN KEY (team_id) REFERENCES Team(sr_id),
        CONSTRAINT season_id_FK FOREIGN KEY (season_id) REFERENCES Season(sr_id)
        )''')

    conn.commit()


def populate_seasons_table(testing, seasons):
    """
        Inserts season records into the season tables. For season_ids, it rotates PRE-season, POST-season, REG-season
        Also note that "2013" is actually the 2013-2014 season
    :param testing: if you are testing or debugging
    :param season_ids: dictionary of sport radar IDs and year for each season and season type (pre, post, reg)
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    season_ids = list(seasons.keys())

    for season_id in season_ids:
        season_year = seasons[season_id]

        # set up
        index_mod = (season_ids.index(season_id)) % 3

        if index_mod == 0:
            season_type = 'PRE'
            name_type = 'Pre-season'
        elif index_mod == 1:
            season_type = 'PST'
            name_type = 'Post-season'
        else:
            season_type = 'REG'
            name_type = 'Regular season'

        fields = [
            str(season_year) + '-' + str(int(season_year) + 1) + ' ' + name_type,  # name
            season_type + str(season_year)[-2:] + str(int(season_year) + 1)[-2:],  # codename
            str(season_year),  # year
            str(season_id),  # sportradar_id
            str(season_type),  # type
        ]
        # print(fields)

        insert_statement = "INSERT OR IGNORE INTO Season (name , codename, year, sr_id , type, add_date, " \
                           "update_date, version ,is_deleted) VALUES (?, ?, ?, ?, ?, DateTime('now'), " \
                           "DateTime('now'), 1, 0);"

        # SQL inserts
        cur = conn.cursor()
        cur.execute(insert_statement, fields)
        conn.commit()


def insert_team(testing, df, season_id):
    """
        insert team data into DB
    :param testing: if you are testing or debugging
    :param df: data frame generated to place into DB
    :param season_id: SportRadar ID for Seasons for the FK
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    for index, row in df.iterrows():
        fields = [
            str(season_id),  # [season_id] text,
            str(row['name']),  # [name] text,
            str(row['market']),  # [market] text,
            str(row['wins']),  # wins int,
            str(row['losses']),  # losses int
            str(row['win_pct']),  # win_pct float
            str(row['points_for']),  # points_for float
            str(row['points_against']),  # points_against float
            str(row['point_diff']),  # point_diff float
            str(row['id']),  # [sr_id] text,
            str(row['sr_id']),  # [sr_internal] text,
            str(row['reference']),  # [reference] int,
        ]

        insert_statement = "INSERT OR IGNORE INTO Team(season_id, name, market, wins, losses, win_pct, points_for, " \
                           "points_against, point_diff, sr_id, sr_internal, reference, add_date, update_date, " \
                           "version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DateTime('now'), " \
                           "DateTime('now'), 1, 0); "

        # SQL inserts
        cur = conn.cursor()
        cur.execute(insert_statement, fields)
        conn.commit()

    return


def insert_team_total(testing, df, season_and_team_ids):
    """
        insert team totals data into DB
    :param testing: if you are testing or debugging
    :param df: data frame generated to place into DB
    :param season_and_team_ids: SportRadar Season ID and Team ID for the FK in the database
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    for index, row in df.iterrows():

        if index % 2 == 0:  # opponent, based on the index of the dataframe
            is_opponent = 0
        else:
            is_opponent = 1

        fields = [
            season_and_team_ids[0],  # season_id
            season_and_team_ids[1],  # team_sr_id
            str(row['games_played']),  # games_played
            is_opponent,  # opponent
            str(row['minutes']),  # minutes
            str(row['field_goals_made']),  # field_goals_made
            str(row['field_goals_att']),  # field_goals_att
            str(row['field_goals_pct']),  # field_goals_pct
            str(row['two_points_made']),  # two_points_made
            str(row['two_points_att']),  # two_points_att
            str(row['two_points_pct']),  # two_points_pct
            str(row['three_points_made']),  # three_points_made
            str(row['three_points_att']),  # three_points_att
            str(row['three_points_pct']),  # three_points_pct
            str(row['blocked_att']),  # blocked_att
            str(row['free_throws_made']),  # free_throws_made
            str(row['free_throws_att']),  # free_throws_att
            str(row['free_throws_pct']),  # free_throws_pct
            str(row['offensive_rebounds']),  # offensive_rebounds
            str(row['defensive_rebounds']),  # defensive_rebounds
            str(row['rebounds']),  # rebounds
            str(row['assists']),  # assists
            str(row['turnovers']),  # turnovers
            str(row['assists_turnover_ratio']),  # assists_turnover_ratio
            str(row['steals']),  # steals
            str(row['blocks']),  # blocks
            str(row['personal_fouls']),  # personal_fouls
            str(row['tech_fouls']),  # tech_fouls
            str(row['points']),  # points
            str(row['fast_break_pts']),  # fast_break_pts
            str(row['flagrant_fouls']),  # flagrant_fouls
            str(row['points_off_turnovers']),  # points_off_turnovers
            str(row['second_chance_pts']),  # second_chance_pts
            str(row['ejections']),  # ejections
            str(row['foulouts']),  # foulouts
            str(row['points_in_paint']),  # points_in_paint
            str(row['efficiency']),  # efficiency
            str(row['true_shooting_att']),  # true_shooting_att
            str(row['true_shooting_pct']),  # true_shooting_pct
            str(row['points_in_paint_made']),  # points_in_paint_made
            str(row['points_in_paint_att']),  # points_in_paint_att
            str(row['points_in_paint_pct']),  # points_in_paint_pct
            str(row['effective_fg_pct']),  # effective_fg_pct
            str(row['bench_points']),  # bench_points
            str(row['fouls_drawn']),  # fouls_drawn
            str(row['offensive_fouls']),  # offensive_fouls
            str(row['team_tech_fouls']),  # team_tech_fouls
            str(row['defensive_assists']),  # defensive_assists
            str(row['fast_break_att']),  # fast_break_att
            str(row['fast_break_made']),  # fast_break_made
            str(row['fast_break_pct']),  # fast_break_pct
            str(row['technical_other']),  # technical_other
            str(row['coach_ejections']),  # coach_ejections
            str(row['points_against']),  # points_against
            str(row['team_defensive_rebounds']),  # team_defensive_rebounds
            str(row['team_offensive_rebounds']),  # team_offensive_rebounds
            str(row['second_chance_att']),  # second_chance_att
            str(row['second_chance_made']),  # second_chance_made
            str(row['second_chance_pct']),  # second_chance_pct
            str(row['coach_tech_fouls']),  # coach_tech_fouls
            str(row['team_fouls']),  # team_fouls
            str(row['total_rebounds']),  # total_rebounds
            str(row['total_fouls'])  # total_fouls
        ]

        insert_statement = "INSERT OR IGNORE INTO TeamTotal(season_id, team_id,games_played,is_opponent,minutes," \
                           "field_goals_made,field_goals_att,field_goals_pct,two_points_made,two_points_att," \
                           "two_points_pct,three_points_made,three_points_att,three_points_pct,blocked_att," \
                           "free_throws_made,free_throws_att,free_throws_pct,offensive_rebounds,defensive_rebounds," \
                           "rebounds,assists,turnovers,assists_turnover_ratio,steals,blocks,personal_fouls," \
                           "tech_fouls,points,fast_break_pts,flagrant_fouls,points_off_turnovers,second_chance_pts," \
                           "ejections,foulouts,points_in_paint,efficiency,true_shooting_att,true_shooting_pct," \
                           "points_in_paint_made,points_in_paint_att,points_in_paint_pct,effective_fg_pct," \
                           "bench_points,fouls_drawn,offensive_fouls,team_tech_fouls,defensive_assists," \
                           "fast_break_att,fast_break_made,fast_break_pct,technical_other,coach_ejections," \
                           "points_against,team_defensive_rebounds,team_offensive_rebounds,second_chance_att," \
                           "second_chance_made,second_chance_pct,coach_tech_fouls,team_fouls,total_rebounds," \
                           "total_fouls,add_date,update_date,version,is_deleted) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?," \
                           "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," \
                           "?,?,?,?,?,DateTime('now'),DateTime('now'), 1, 0); "

        cur = conn.cursor()
        cur.execute(insert_statement, fields)
        conn.commit()


def insert_team_average(testing, df, season_and_team_ids):
    """
        insert team averages data into DB
    :param testing: if you are testing or debugging
    :param df: data frame generated to place into DB
    :param season_and_team_ids: SportRadar Season ID and Team ID for the FK in the database
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    for index, row in df.iterrows():

        if index % 2 == 0:  # opponent, based on the index of the dataframe
            is_opponent = 0
        else:
            is_opponent = 1

        fields = [
            season_and_team_ids[0],  # season_id
            season_and_team_ids[1],  # team_sr_id
            is_opponent,  # is_opponent
            str(row['fast_break_pts']),  # fast_break_pts
            str(row['points_off_turnovers']),  # points_off_turnovers
            str(row['second_chance_pts']),  # second_chance_pts
            str(row['minutes']),  # minutes
            str(row['points']),  # points
            str(row['off_rebounds']),  # off_rebounds
            str(row['def_rebounds']),  # def_rebounds
            str(row['rebounds']),  # rebounds
            str(row['assists']),  # assists
            str(row['steals']),  # steals
            str(row['blocks']),  # blocks
            str(row['turnovers']),  # turnovers
            str(row['personal_fouls']),  # personal_fouls
            str(row['flagrant_fouls']),  # flagrant_fouls
            str(row['blocked_att']),  # blocked_att
            str(row['field_goals_made']),  # field_goals_made
            str(row['field_goals_att']),  # field_goals_att
            str(row['three_points_made']),  # three_points_made
            str(row['three_points_att']),  # three_points_att
            str(row['free_throws_made']),  # free_throws_made
            str(row['free_throws_att']),  # free_throws_att
            str(row['two_points_made']),  # two_points_made
            str(row['two_points_att']),  # two_points_att
            str(row['points_in_paint']),  # points_in_paint
            str(row['efficiency']),  # efficiency
            str(row['true_shooting_att']),  # true_shooting_att
            str(row['points_in_paint_att']),  # points_in_paint_att
            str(row['points_in_paint_made']),  # points_in_paint_made
            str(row['bench_points']),  # bench_points
            str(row['fouls_drawn']),  # fouls_drawn
            str(row['offensive_fouls']),  # offensive_fouls
            str(row['fast_break_att']),  # fast_break_att
            str(row['fast_break_made']),  # fast_break_made
            str(row['second_chance_att']),  # second_chance_att
            str(row['second_chance_made']),  # second_chance_made
        ]

        insert_statement = "INSERT OR IGNORE INTO TeamAverage(season_id,team_id,is_opponent,fast_break_pts," \
                           "points_off_turnovers,second_chance_pts,minutes,points,off_rebounds,def_rebounds,rebounds," \
                           "assists,steals,blocks,turnovers,personal_fouls,flagrant_fouls,blocked_att," \
                           "field_goals_made,field_goals_att,three_points_made,three_points_att,free_throws_made," \
                           "free_throws_att,two_points_made,two_points_att,points_in_paint,efficiency," \
                           "true_shooting_att,points_in_paint_att,points_in_paint_made,bench_points,fouls_drawn," \
                           "offensive_fouls,fast_break_att,fast_break_made,second_chance_att,second_chance_made," \
                           "add_date,update_date,version,is_deleted) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," \
                           "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,DateTime('now'),DateTime('now'), 1, 0); "

        cur = conn.cursor()
        cur.execute(insert_statement, fields)
        conn.commit()


def insert_player(testing, df, team_id, season_id, total_df, average_df):
    """
        insert player data into DB
    :param testing: if you are testing or debugging
    :param df: data frame generated to place into DB
    :param team_id: SportRadar Team ID for the FK in the database
    :param season_id: SportRadar Season ID for second FK in db
    :return player_id: so that the PlayerTotal and PlayerAverage functions are called, they have the player_id
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    for index, row in df.iterrows():
        # print(row)
        fields = [
            str(team_id),  # [team_id] text
            str(season_id),  # [season_id] text
            str(row['id']),  # [sr_id] text
            str(row['first_name']),  # first_name
            str(row['last_name']),  # last_name
            str(row['position']),  # position
            str(row['primary_position']),  # primary_position
            str(row['jersey_number']),  # jersey_number
            str(row['sr_id']),  # [sr_internal] text
            str(row['reference'])  # [reference] int
        ]

        # print(fields)

        insert_statement = "INSERT OR IGNORE INTO Player(team_id, season_id, sr_id, first_name, last_name, position, " \
                           "primary_position, jersey_number, sr_internal, reference, add_date, update_date, version, " \
                           "is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DateTime('now'), DateTime('now'), 1, 0); "

        # print(insert_statement, fields)
        # SQL inserts
        cur = conn.cursor()
        cur.execute(insert_statement, fields)
        conn.commit()

        insert_player_total(testing, total_df, team_id, season_id)
        insert_player_average(testing, average_df, team_id, season_id)


def insert_player_total(testing, df, team_id, season_id):
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

    for index, row in df.iterrows():
        fields = [
            str(row['id']),  # player_id
            str(team_id),
            str(season_id),
            str(row['games_played']),  # games_played
            str(row['games_started']),  # games_started
            str(row['minutes']),  # minutes
            str(row['field_goals_made']),  # field_goals_made
            str(row['field_goals_att']),  # field_goals_att
            str(row['field_goals_pct']),  # field_goals_pct
            str(row['two_points_made']),  # two_points_made
            str(row['two_points_att']),  # two_points_att
            str(row['two_points_pct']),  # two_points_pct
            str(row['three_points_made']),  # three_points_made
            str(row['three_points_att']),  # three_points_att
            str(row['three_points_pct']),  # three_points_pct
            str(row['blocked_att']),  # blocked_att
            str(row['free_throws_made']),  # free_throws_made
            str(row['free_throws_att']),  # free_throws_att
            str(row['free_throws_pct']),  # free_throws_pct
            str(row['offensive_rebounds']),  # offensive_rebounds
            str(row['defensive_rebounds']),  # defensive_rebounds
            str(row['rebounds']),  # rebounds
            str(row['assists']),  # assists
            str(row['turnovers']),  # turnovers
            str(row['assists_turnover_ratio']),  # assists_turnover_ratio
            str(row['steals']),  # steals
            str(row['blocks']),  # blocks
            str(row['personal_fouls']),  # personal_fouls
            str(row['tech_fouls']),  # tech_fouls
            str(row['points']),  # points
            str(row['flagrant_fouls']),  # flagrant_fouls
            str(row['ejections']),  # ejections
            str(row['foulouts']),  # foulouts
            str(row['true_shooting_att']),  # true_shooting_att
            str(row['true_shooting_pct']),  # true_shooting_pct
            str(row['efficiency']),  # efficiency
            str(row['points_off_turnovers']),  # points_off_turnovers
            str(row['points_in_paint']),  # points_in_paint
            str(row['points_in_paint_made']),  # points_in_paint_made
            str(row['points_in_paint_att']),  # points_in_paint_att
            str(row['points_in_paint_pct']),  # points_in_paint_pct
            str(row['effective_fg_pct']),  # effective_fg_pct
            str(row['double_doubles']),  # double_doubles
            str(row['triple_doubles']),  # triple_doubles
            str(row['fouls_drawn']),  # fouls_drawn
            str(row['offensive_fouls']),  # offensive_fouls
            str(row['fast_break_pts']),  # fast_break_pts
            str(row['fast_break_att']),  # fast_break_att
            str(row['fast_break_made']),  # fast_break_made
            str(row['fast_break_pct']),  # fast_break_pct
            str(row['coach_ejections']),  # coach_ejections
            str(row['second_chance_pct']),  # second_chance_pct
            str(row['second_chance_pts']),  # second_chance_pts
            str(row['second_chance_att']),  # second_chance_att
            str(row['second_chance_made']),  # second_chance_made
            str(row['minus']),  # minus
            str(row['plus']),  # plus
            str(row['coach_tech_fouls'])  # coach_tech_fouls
        ]
        # print(fields)

        insert_statement = "INSERT OR IGNORE INTO PlayerTotal(player_id,team_id,season_id,games_played,games_started," \
                           "minutes,field_goals_made,field_goals_att,field_goals_pct,two_points_made,two_points_att," \
                           "two_points_pct,three_points_made,three_points_att,three_points_pct,blocked_att," \
                           "free_throws_made,free_throws_att,free_throws_pct,offensive_rebounds,defensive_rebounds," \
                           "rebounds,assists,turnovers,assists_turnover_ratio,steals,blocks,personal_fouls," \
                           "tech_fouls,points,flagrant_fouls,ejections,foulouts,true_shooting_att,true_shooting_pct," \
                           "efficiency,points_off_turnovers,points_in_paint,points_in_paint_made,points_in_paint_att," \
                           "points_in_paint_pct,effective_fg_pct,double_doubles,triple_doubles,fouls_drawn," \
                           "offensive_fouls,fast_break_pts,fast_break_att,fast_break_made,fast_break_pct," \
                           "coach_ejections,second_chance_pct,second_chance_pts,second_chance_att,second_chance_made," \
                           "minus,plus,coach_tech_fouls,add_date,update_date,version,is_deleted) VALUES (?,?,?,?,?,?," \
                           "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," \
                           "?,?,?,?,?,?,?,?, DateTime('now'), DateTime('now'), 1, 0); "

        # SQL inserts
        cur = conn.cursor()
        cur.execute(insert_statement, fields)
        conn.commit()


def insert_player_average(testing, df, team_id, season_id):
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

    for index, row in df.iterrows():
        fields = [
            str(row['id']),  # player_id
            str(team_id),
            str(season_id),
            str(row['minutes']),  # minutes
            str(row['points']),  # points
            str(row['off_rebounds']),  # off_rebounds
            str(row['def_rebounds']),  # def_rebounds
            str(row['rebounds']),  # rebounds
            str(row['assists']),  # assists
            str(row['steals']),  # steals
            str(row['blocks']),  # blocks
            str(row['turnovers']),  # turnovers
            str(row['personal_fouls']),  # personal_fouls
            str(row['flagrant_fouls']),  # flagrant_fouls
            str(row['blocked_att']),  # blocked_att
            str(row['field_goals_made']),  # field_goals_made
            str(row['field_goals_att']),  # field_goals_att
            str(row['three_points_made']),  # three_points_made
            str(row['three_points_att']),  # three_points_att
            str(row['free_throws_made']),  # free_throws_made
            str(row['free_throws_att']),  # free_throws_att
            str(row['two_points_made']),  # two_points_made
            str(row['two_points_att']),  # two_points_att
            str(row['efficiency']),  # efficiency
            str(row['true_shooting_att']),  # true_shooting_att
            str(row['points_off_turnovers']),  # points_off_turnov
            str(row['points_in_paint_made']),  # points_in_paint_m
            str(row['points_in_paint_att']),  # points_in_paint_a
            str(row['points_in_paint']),  # points_in_paint
            str(row['fouls_drawn']),  # fouls_drawn
            str(row['offensive_fouls']),  # offensive_fouls
            str(row['fast_break_pts']),  # fast_break_pts
            str(row['fast_break_att']),  # fast_break_att
            str(row['fast_break_made']),  # fast_break_made
            str(row['second_chance_pts']),  # second_chance_pts
            str(row['second_chance_att']),  # second_chance_att
            str(row['second_chance_made'])  # second_chance_mad
        ]

        insert_statement = "INSERT OR IGNORE INTO PlayerAverage(player_id,team_id,season_id,minutes,points," \
                           "off_rebounds,def_rebounds,rebounds,assists,steals,blocks,turnovers,personal_fouls," \
                           "flagrant_fouls,blocked_att,field_goals_made,field_goals_att,three_points_made," \
                           "three_points_att,free_throws_made,free_throws_att,two_points_made,two_points_att," \
                           "efficiency,true_shooting_att,points_off_turnovers,points_in_paint_made," \
                           "points_in_paint_att,points_in_paint,fouls_drawn,offensive_fouls,fast_break_pts," \
                           "fast_break_att,fast_break_made,second_chance_pts,second_chance_att,second_chance_made," \
                           "add_date,update_date,version,is_deleted) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," \
                           "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, DateTime('now'), DateTime('now'), 1, 0); "

        cur = conn.cursor()
        cur.execute(insert_statement, fields)
        conn.commit()


def clean_up_players(testing):
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    possible_fields = ['jersey_number', 'sr_internal', 'reference']
    for field in possible_fields:
        if field == 'primary_position':
            update_statement = "UPDATE Player SET " + field + " = NULL WHERE " + field + " IN ('_None_','NA'); "
        else:
            update_statement = "UPDATE Player SET " + field + " = NULL WHERE " + field + " = '_None_';"

        cur = conn.cursor()
        cur.execute(update_statement)
        conn.commit()
