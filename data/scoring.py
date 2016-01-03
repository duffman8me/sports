'''
This library is intended to house higher level functions for pulling data
from the database and mapping scores to historical salaries.

'''
import pymongo as pm
import pandas as pd
import seaborn as sns
from sports.data.database import read_salary_info, league_map, read_current_info
import matplotlib.pyplot as plt

nba_scoring = {'dk_scoring':  {'points': 1,
                               'three pointers made': .5,
                               'rebounds': 1.25,
                               'assists': 1.5,
                               'steals': 2,
                               'blocks': 2,
                               'turnovers': -.5,
                               'double': 1.5,
                               'triple': 3.0
                           },
               'fd_scoring' : {'field goals made': 2.0,
                               'three pointers made': 3.0,
                               'free throws made': 1.0,
                               'assists': 1.5,
                               'steals': 2,
                               'blocks': 2,
                               'turnovers': -1.0,
                               'rebounds': 1.2,
                               'double': 0.0,
                               'triple': 0.0
                           }
           }

scoring = {'nba': nba_scoring}

class HistoricalScore(object):
    
    def __init__(self, league, **kwargs):
        if 'custom_score' in kwargs.keys():
            self.scoring = kwargs['custom_score']
        else:
            self.scoring = scoring[league]        
        self.league = league
        self._load_data(**kwargs)
        

    def _load_data(self, sDt=None, eDt=None):
        query = self._get_dates(sDt=sDt, eDt=eDt)

        db = league_map[self.league]

        df = pd.DataFrame(list(db['player_data'].find(query)))
        df_salary = read_salary_info()
        df_salary = df_salary[['dk salary','fd salary']]

        self.hist_data = df
        self.salary = df_salary

    def _get_dates(self,**kwargs):
        query = {}
        sDt, eDt = kwargs.get('sDt'), kwargs.get('eDt')
        if sDt is not None:
            sDt = pd.Timestamp(sDt)
            existing_time_query = query.get('date',{})
            existing_time_query.update({'$gte': sDt.to_pydatetime()})
            query['date'] = existing_time_query
            
        if eDt is not None:
            eDt = pd.Timestamp(eDt)
            existing_time_query = query.get('date',{})
            existing_time_query.update({'$lte': eDt.to_pydatetime()})
            query['date'] = existing_time_query

        return query
            
    def get_score(self, sDt=None, eDt=None, game='fd', **kwargs):
        '''
        get scoring data from database.  Indexed by date, team, and player name.

        arguments:
        sDt       - start date for games
        eDt       - end date for games
        game      - which fantasy game to score ('fd' or 'dk' for now.)

        '''
        df = self.hist_data
        scoring = pd.Series(self.scoring[game+'_scoring'])
        scored_d = df.set_index(['date','team','name'])[[dc for dc in scoring.keys()
                                                         if dc in df.columns]]
        scored_d['double'] = ((scored_d >= 10.0).sum(1) > 1) * 1.0
        scored_d['triple'] = ((scored_d >= 10.0).sum(1) > 2) * 1.0

        scores = scored_d.mul(scoring).sum(1).sort_index()
        if sDt is not None:
            scores = scores.ix[pd.Timestamp(sDt):]
        if eDt is not None:
            scores = scores.ix[:pd.Timestamp(eDt)]
        
        return scores

    def get_salary(self, sDt=None, eDt=None, game='fd', **kwargs):
        '''
        get salary data indexed by date, player.

        arguments:
        sDt       - start date for games
        eDt       - end date for games
        game      - which fantasy game to score ('fd' or 'dk' for now.)
        
        '''
        salary_d = self.salary
        if sDt is not None:
            salary_d = salary_d.ix[pd.Timestamp(sDt):]
        if eDt is not None:
            salary_d = salary_d.ix[:pd.Timestamp(eDt)]
        if game == 'fd':
            return salary_d['fd salary']
        if game == 'dk':
            return salary_d['dk salary']

    def join_salary_score_data(self, **kwargs):
        '''
        Attempt at aligning score and salary data.  
        '''
        sal = self.get_salary(**kwargs)
        scores = self.get_score(**kwargs)
        sal_f = sal.to_frame()
        sal_f.name = 'salary'
        scores.name = 'score'
        joined = sal_f.join(scores.reset_index(level='team'), how='left')
        joined['team'] = joined['team'].unstack(-1).fillna(method='ffill').stack()
        joined['score'] = joined['score'].fillna(0.0)
        joined = joined.set_index('team',append=True).reorder_levels(['date','team','name']).\
                 sort_index()
        return joined

    def get_latest_salaries(self, **kwargs):
        '''
        This pulls from the most recent historical data to get salaries.
        Use get_upcoming_info to get current salaries and position info.
        '''
        joined_d = self.join_salary_score_data(**kwargs)
        proxy_s = joined_d.groupby(level='name').last().filter(regex=' salary')
        return proxy_s

    def get_upcoming_info(self):
        return read_current_info(league=self.league)
        
    def get_score_cov_mat(self, team, corr_mat=False, **kwargs):
        '''
        This probably belongs in a wrapper class that takes a historical
        data object as an input and produces statistics / metrics on which
        to operate.
        '''
        result = self.get_score(**kwargs)
        result = result.xs(team, level='team')
        if corr_mat:
            return result.unstack('name').corr()
        else:
            return result.unstack('name').cov()

    def get_cov_heat_map(self, *args, **kwargs):
        result = self.get_score_cov_mat(*args,**kwargs)
        sns.heatmap(result)
        plt.show()

    def build_cov_mat(self, players):
        pass
            
def get_poss_portfolios(all_players, limit=50000.0):
    '''
    This is pretty much an intractable problem and we need to think of ways
    of simplifying the analysis.
    '''
    player_combos = []

    def subset_sum(numbers, target, curr_nums=pd.Series()):
        s = curr_nums.sum()

        if s >= target:
            player_combos.append(curr_nums.index)

        for i in range(len(numbers)):
            n = numbers.iloc[[i]]
            remaining = numbers[i+1:]
            subset_sum(remaining, target, curr_nums.append(n))

    subset_sum(all_players.iloc[:15], limit)
    nums = range(10000,50000,500)     
    all_players = pd.Series(nums,index=nums)
    subset_sum(all_players.iloc[:5], limit)