import pandas as pd
from BeautifulSoup import BeautifulSoup
import requests
import re
from StringIO import StringIO


urls = {'nba': 'https://www.numberfire.com/nba/fantasy/full-fantasy-basketball-projections'}

def get_current_roster(league='nba'):
    main_url = urls[league]

    bs = BeautifulSoup(StringIO(requests.get(main_url).text))

    all_txt = bs.find(text=re.compile('fanduel_fp'))
    json_txt = re.search('var NF_DATA = (.*)', all_txt)

    txt = json_txt.group(1)[:-2]
    txt = json.loads(txt)
    data = {kk: pd.DataFrame(vv) for kk, vv in txt.iteritems() if kk in ['daily_projections','players','daily_projections']}
    curr_roster = data['daily_projections'].set_index('nba_player_id')
    all_players = data['players'].T.set_index('id')