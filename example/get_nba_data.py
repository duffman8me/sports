'''
Meant to be a working script to demonstrate higher level
usage of data interface and experimental data analysis.

This should run without any required initialization of database.
The only stipulation is that an instnace of the MongoDB server
must be running.

'''

from sports.data.scoring import HistoricalScore
from sports.data import populate_database as pop_db
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Demonstrate higher level data analysis.')
    parser.add_argument('-d','--download',
                        help='Update database first.  This can take awhile.',
                        action="store_true"
    )
    args = parser.parse_args()

    if bool(args.download):
        pop_db.update_all_leagues(['nba'],[2014,2015])
    
    hs = HistoricalScore('nba')

    hs.get_cov_heat_map('Bulls',sDt='01-Jan-2015')
    plt.show()

    game = 'dk'
    data = hs.join_salary_score_data(sDt='01-Jan-2015', game=game)
    sns.jointplot(x=game + ' salary', y='score', data=data.groupby(level=2).mean())
    plt.show()