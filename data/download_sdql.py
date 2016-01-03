from requests import request
import urllib
import pandas as pd
import pdb
import numpy as np
from sports.data.external import rotoguru, rotowire

def _data_from_url_dict(url, data_dict):
    new_url = url + urllib.urlencode(data_dict)
    return request('GET', new_url)

def _generate_url_dict_from_sdql(sdql):
    return_dict = {'output': 'json',
                   'api_key': 'guest'}

    return_dict.update({'sdql': sdql})
    return return_dict

def _process_returned_data(json):
    new_json = eval(json.replace('json_callback(','').replace(');\n','').replace('null',"'null'"))
    if new_json == 'null':
        return pd.DataFrame()
    column_labels = [str(hh).strip() for hh in new_json['headers']]
    ret_dict = {}
    for col_info in new_json['groups']:
        raw_d = np.array(col_info['columns']).T
        df = pd.DataFrame(raw_d, columns=column_labels)
        for col in column_labels:
            df[col] = df[col].replace('null',np.nan)
            if col == 'date':
                df[col] = pd.DatetimeIndex(df[col])
            else:
                try:
                    df[col] = df[col].astype(float)
                except ValueError, vee:
                    pass # leave as string.
        ret_dict.update({col_info['sdql']: df})
        
    return ret_dict

def team_data_from_sdql(sdql, league):
    url = 'http://api.sportsdatabase.com/{league}/query.json?'.format(**locals())
    return data_from_sdql(sdql, url)

def player_data_from_sdql(sdql, league):
    url = 'http://api.sportsdatabase.com/{league}/player_query.json?'.format(**locals())
    return data_from_sdql(sdql, url)
    
def data_from_sdql(sdql,url):
    arg_dict = _generate_url_dict_from_sdql(sdql)
    return_data = _data_from_url_dict(url, arg_dict)
    return _process_returned_data(return_data.text)

def get_unique_teams(season=2014):
    if hasattr(season, '__iter__'):
        return pd.concat([get_unique_teams(ss) for ss in season], axis=1)
    else:        
        season = str(season)
        df = team_data_from_sdql('Unique(team)@season={season}'.format(**locals()), 'nba')

        df = df.values()[0]
        df.columns = ['teams_{season}'.format(**locals())]

        return np.squeeze(df)

