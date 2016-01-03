from BeautifulSoup import BeautifulSoup
import pandas as pd
import requests
import pdb
import re

def get_current_lineups(league='nba',site='DraftKings'):
    link = 'http://www.rotowire.com/daily/{league}/optimizer.htm?site={site}'.format(**locals())
    bs = BeautifulSoup(requests.get(link).text)

    all_player_d = {}
    for row in bs.findAll(name='tr', attrs={'class': 'playerSet'}):
        nm = row.find(attrs={'class': re.compile('lineupopt-name')}).text
        if '&nbsp;Out' in nm:
            nm = nm.split('&nbsp;Out')[0]
            all_player_d[nm] = {'out': True,
                                'gtd': False
            }
        elif '&nbsp;GTD' in nm:
            nm = nm.split('&nbsp;GTD')[0]
            all_player_d[nm] = {'out': False,
                                'gtd': True
            }
        else:
            all_player_d[nm] = {'out': False,
                                'gtd': False
            }
            
        for cell in row:
            if not (cell == '\n') and len(cell.text):
                key = re.search('lineupopt-(.*)', cell['class']).group(1)
                all_player_d[nm][key] = cell.text

    ret_d = pd.DataFrame(all_player_d).T.drop('name',axis=1)
    ret_d.index = ret_d.index.set_names(['name'])
    if league == 'nba':
        dt_txt = bs.find(text=re.compile('NBA Daily Lineup Optimizer &ndash'))
    dt = pd.Timestamp(dt_txt.split('&ndash;')[-1].strip().replace('&nbsp;',' '))
    ret_d['date'] = dt
    return ret_d