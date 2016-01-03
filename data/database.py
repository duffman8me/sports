'''
Database handling functions for writing to and reading from
MongoDB infrastructure.  Will likely become its own directory
as project is scaled.
'''
from pymongo import MongoClient
import pandas as pd
from copy import copy
from sports.util.str_help import compare_two_strings
import pdb

nba_db = MongoClient()['sports_nba']

league_map = {'nba': nba_db}


def write_player_data(df, league='nba', collection = None):
    if collection is None:
        collection = league_map[league]['player_data']

    try:
        df2 = df.reset_index().set_index(['name','date','team'])
    except Exception, ee:
        raise RuntimeError('Written player data requires name, date, and team information.')
        
    _write_data(df2,collection)
        
def write_game_data(df, league='nba', collection = None):
    if collection is None:
        collection = league_map[league]['game_data']

    try:
        df2 = df.reset_index().set_index(['date','team'])
    except Exception, ee:
        raise RuntimeError('Written game data requires date and team information.')
        
    _write_data(df2,collection)

def write_salary_info(df, league='nba', collection = None):
    if collection is None:
        collection = league_map[league]['salary_data']

    try:
        df2 = df.reset_index().set_index(['name','date'])
    except Exception, ee:
        raise RuntimeError('Written salary data requires date and name information')

    _write_data(df2,collection)

def write_current_info(df, league='nba', collection = None):
    '''
    This reads from current_data in database.  Plan is to have a separate table
    that houses latest info and models can read from that at any time and
    safely assume it contains the most recent data for upcoming games.
    '''
    if collection is None:
        collection = league_map[league]['current_data']

    try:
        df2 = df.reset_index().set_index(['name','date'])
    except Exception, ee:
        raise RuntimeError('Written salary data requires date and name information')

    _write_data(df2,collection)

def map_salary_names(league='nba'):
    '''
    This is intended to map the salary information coming from a different source
    than the player info.  It updates the database with this mapping.
    '''
    db = league_map[league]
    salary_names = db['salary_data'].find({},{'name': 1}).distinct('name')
    player_names = db['player_data'].find({},{'name': 1}).distinct('name')

    for original in salary_names:
        info = compare_two_strings(' '.join(str(original).split(', ')[::-1]),player_names)
        new_record = {'salary_name': original,
                      'player_name': info['candidate'].item(),
                      'ratio': info['ratio'].item()
        }
        existing_record = db['salary_player_map'].find_one({'salary_name': original})
        if existing_record is None or existing_record['ratio'] < new_record['ratio']:
            db['salary_player_map'].update_one({'salary_name': original},
                                               {'$set': new_record},
                                               upsert=True
            )
    
        
def _write_data(df, collection):
    ind_recs = pd.DataFrame(df.index.tolist(),columns=df.index.names).to_dict('records')

    for ind_rec, new_rec in zip(ind_recs, df.reset_index().to_dict('records')):
        collection.update_one(_clean_data(ind_rec),
                              {'$set': _clean_data(new_rec)},upsert=True)

def _clean_data(dd):
    clean = copy(dd)
    for key, val in dd.iteritems():
        if type(val) == pd.Timestamp:
            clean[key] = val.to_pydatetime()

    return clean

def read_salary_info(*args, **kwargs):
    if 'league' in kwargs.keys():
        league = kwargs.get('league', 'nba')
        del kwargs['league']
    else:
        league = 'nba'

    if 'collection' in kwargs.keys():
        collection = kwargs.get('collection')
        del kwargs['collection']
    else:
        collection = league_map[league]['salary_data']

    all_players = league_map[league]['player_data'].find({},{'name': 1}).distinct('name')
    try:
        df_salary = pd.DataFrame(list(collection.find(kwargs,{'fd salary': 1,
                                                              'dk salary': 1,
                                                              'name':1,
                                                              'date': 1,
                                                              '_id': 0
                                                      })))
        
    except Exception, ee:
        raise RuntimeError('Cannot extract salary data.')

    mapper = get_player_name_mapper(db = collection.database)    
    df_salary.index = pd.MultiIndex.from_arrays([df_salary['date'],
                                                 df_salary['name'].map(mapper).values],
                                                names = ['date','name'])
    df_salary = df_salary.ix[~df_salary.index.get_level_values('name').to_series().isnull().values]
    return df_salary.sort_index()

def get_player_name_mapper(db):
    mapper = pd.DataFrame(list(db['salary_player_map'].find()))
    # This asserts uniqueness of mapping.  i.e. one to one and never many to one
    mapper = mapper.sort('ratio',ascending=False).reset_index().groupby('player_name').first()
    mapper = mapper.reset_index('player_name').set_index('salary_name')['player_name']
    assert (not mapper.duplicated().any()) and (mapper.index.is_unique)
    return mapper
    
def read_current_info(league='nba', **kwargs):
    query = {}
    query.update(kwargs)
    db = league_map[league]
    ret_d = pd.DataFrame(list(db['current_data'].find(query,{'_id': 0}))).sort('name').reset_index(drop=True)
    mapper = get_player_name_mapper(db)
    ret_d['name'] = ret_d['name'].map(mapper)
    return ret_d
    