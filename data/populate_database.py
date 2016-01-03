'''
Main script for initializing and downloading all data (including score
and salary information).  Run this first and anytime you want to scrape
the most current information.
'''

from sports.data import database as dbase
from sports.data import leagues
import argparse
import pandas as pd
import os, types

def update_all_leagues(ls, yrs):
    for ll in ls:
        d_score_func = eval('leagues.{0}.get_all_player_data'.format(ll))
        d_salary_func = eval('leagues.{0}.update_salary_info'.format(ll))

        for yr in yrs:
            sDt = pd.Timestamp('06-01-{yr}'.format(**locals())) - pd.offsets.YearBegin(1)
            eDt = sDt + pd.offsets.YearEnd(1)            
            d_score_func(yr)
            d_salary_func(sDt, eDt)

if __name__ == '__main__':
    eDt = pd.Timestamp.today()        
    sDt = pd.Timestamp(eDt.date()) - pd.offsets.YearBegin(2)

    #This is a bit naive and might blow up... but shouldn't hurt anything.
    league = [str(kk) for kk, vv in leagues.__dict__.iteritems() if isinstance(vv, types.ModuleType)]

    parser = argparse.ArgumentParser(description='Update all database information.')
    parser.add_argument('--start', default=sDt, nargs='?', help='Start date to download data.')
    parser.add_argument('--end', default=eDt, nargs='?', help='Start date to download data.')
    parser.add_argument('--league', default=league, help='League(s) to download.')

    args = parser.parse_args()
    sDt = pd.Timestamp(args.start)
    eDt = pd.Timestamp(args.end)
    
    yrs = [dt.year for dt in pd.date_range(sDt, eDt, freq='AS')]
    update_all_leagues(args.league, yrs)
    
        

