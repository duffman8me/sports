'''
Script that SHOULD implement an interface for housing various data
download functions and retrieval from external websites into database.
This is a major TODO, such that other sports are easily implemented and
added to the database.
'''

from sports.data import download_sdql as dload
from sports.data import database as dbase
import pandas as pd

def update_all_nba_player_data(team, season, store=False):
    player_stats = ['team','date','assists', 'blocks', 'defensive rebounds',
                    'field goals attempted', 'field goals made',
                    'fouls', 'free throws attempted', 'free throws made',
                    'minutes', 'name', 'offensive rebounds', 'plus minus',
                    'points', 'position', 'rebounds', 'steals', 'three pointers attempted',
                    'three pointers made', 'turnovers']

    season = str(season)
    team = str(team).capitalize()
    sdql = ','.join(player_stats) + '@season={season} and team={team}'.format(**locals())

    ret_d = dload.player_data_from_sdql(sdql, 'nba')
    
    if type(ret_d) == dict:
        ret_d = ret_d.values()[0]
    elif type(ret_d) == pd.DataFrame:
        if ret_d.empty:
            return None
    if store:
        dbase.write_player_data(ret_d)

    return ret_d

def retrieve_all_nba_game_data(team, season):
    game_stats = ['assists', 'ats margin',
                  'ats streak', 'attendance',
                  'biggest lead', 'blocks',
                  'conference', 'date', 'day',
                  'defensive rebounds', 'division',
                  'dpa', 'dps', 'fast break points',
                  'field goals attempted', 'field goals made',
                  'fouls', 'free throws attempted',
                  'free throws made', 'game number',
                  'lead changes', 'line', 'losses',
                  'margin', 'margin after the first',
                  'margin after the third',
                  'margin at the half', 'matchup losses',
                  'matchup wins', 'minutes', 'month',
                  'offensive rebounds', 'officials',
                  'opponents', 'ou margin', 'ou streak',
                  'overtime', 'playoffs', 'points',
                  'points in the paint', 'position',
                  'quarter scores', 'rebounds', 'rest',
                  'round', 'season', 'seed', 'series game',
                  'series games', 'series losses',
                  'series wins', 'site', 'site streak',
                  'steals', 'streak', 'team',
                  'team rebounds', 'three pointers attempted',
                  'three pointers made', 'time of game',
                  'time zone', 'times tied', 'total',
                  'turnovers', 'wins']
    season = str(season)
    team = str(team).capitalize()
    sdql = ','.join(game_stats) + '@season={season} and team={team}'.format(**locals())

    ret_d = dload.team_data_from_sdql(sdql, 'nba')
    if type(ret_d) == dict:
        ret_d = ret_d.values()[0]
    elif type(ret_d) == pd.DataFrame:
        if ret_d.empty:
            return None
    if store:
        dbase.write_team_data(ret_d)

    return ret_d
        
def get_unique_teams(season=2014):
    if hasattr(season, '__iter__'):
        return pd.concat([get_unique_teams(ss) for ss in season], axis=1)
    else:        
        season = str(season)
        df = dload.team_data_from_sdql('Unique(team)@season={season}'.format(**locals()), 'nba')

        df = df.values()[0]
        df.columns = ['teams_{season}'.format(**locals())]

        return df

def get_all_player_data(season=2014):
    '''
    Main script for downloading and storing all player stat data.
    '''
    unq_teams = get_unique_teams(season)

    for col in unq_teams:
        season = str(col.split('_')[1])
        for team in unq_teams[col].dropna():
            print('retrieving data for {team}, {season}'.format(**locals()))
            update_all_nba_player_data(team, season, store=True)

# IS THIS USED ANYWHERE?
# def append_team_data(season=2014):
            
#     df = dload.player_data_from_sdql('team,name,date@season={season}'.format(**locals()),'nba')
#     df = df.values()[0]

#     db = dbase.MongoClient()['sports_nba']['player_data']
#     for ii, new_rec in df.drop_duplicates().iterrows():
#         ind_r = {rr: new_rec[rr] for rr in ['name','date']}
#         ind_r['date'] = ind_r['date'].to_pydatetime()

#         db.update(ind_r,{'$set': {'team': new_rec['team']}}, multi=False, upsert=False)

def retrieve_all_nba_player_data(team, season):
    player_stats = ['team','date','assists', 'blocks', 'defensive rebounds',
                    'field goals attempted', 'field goals made',
                    'fouls', 'free throws attempted', 'free throws made',
                    'minutes', 'name', 'offensive rebounds', 'plus minus',
                    'points', 'position', 'rebounds', 'steals', 'three pointers attempted',
                    'three pointers made', 'turnovers']

    season = str(season)
    team = str(team).capitalize()
    sdql = ','.join(player_stats) + '@season={season} and team={team}'.format(**locals())

    return player_data_from_sdql(sdql, 'nba')

def update_salary_info(sDt, eDt):
    '''
    This will write salary info from rotoguru between two specified
    dates.  This is different from update_current_salary_info, which
    pulls from a different source (rotowire) to get the most recent
    salary information and populates the current_data table with this
    info.
    '''
    sDt = pd.Timestamp(sDt)
    eDt = pd.Timestamp(eDt)

    df = dload.rotoguru.get_stats(sDt,eDt,league='nba',game='dk')
    dbase.write_salary_info(df)
    df = dload.rotoguru.get_stats(sDt,eDt,league='nba',game='fd')
    dbase.write_salary_info(df)

def update_current_salary_info(site='DraftKings'):
    df = dload.rotowire.get_current_lineups(league='nba', site=site)
    dbase.write_current_info(df)
    