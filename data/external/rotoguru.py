import pandas as pd
from BeautifulSoup import BeautifulSoup
import requests
import re
from StringIO import StringIO


def get_stats_for_day(dt, league='nba', game='fd'):
    dt = pd.Timestamp(dt)

    hyperlink = 'http://www.rotoguru1.com/cgi-bin/{league}.pl?game={game}&mon={0}&day={1}&year={2}&scsv=1'

    if league in ('nba','mlb'):
        if league == 'nba':
            link = hyperlink.format(dt.strftime('%m'),
                                         dt.strftime('%d'),
                                         dt.strftime('%Y'),
                                         game=game,
                                         league='hyday')
        elif league == 'mlb':
            link = hyperlink.format(dt.strftime('%m'),
                                         dt.strftime('%d'),
                                         dt.strftime('%Y'),
                                         game=game,
                                         league='byday')


    rr = requests.get(link)

    if rr.status_code == 200:
        bs = BeautifulSoup(rr.text)        
        if bs.find(text=re.compile('Data is not available')) is not None:
            return pd.DataFrame()

        rel_text = bs.find('table', text=re.compile('Date;GID.*')) # why does this return text?
        df = pd.read_csv(StringIO(rel_text), sep=';')

        df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
        df.columns = [dd.lower() for dd in df.columns]
        df = df.set_index(['date', 'name'])
        if 'fd salary' in df.columns:
            mask = df['fd salary'].isnull()
            if not mask.all():
                df.loc[~mask,'fd salary'] = df.ix[~mask,'fd salary'].str.replace('$','').\
                                            str.replace(',','').astype(float)
        if 'dk salary' in df.columns:
            mask = df['dk salary'].isnull()
            if not mask.all():
                df.loc[~mask, 'dk salary'] = df.ix[~mask,'dk salary'].str.replace('$','').\
                                             str.replace(',','').astype(float)
        return df
    else:
        print rr.text

        return None

def get_stats(sDt, eDt, **kwargs):
    d_rng = pd.date_range(sDt, eDt, freq='D')

    df = pd.DataFrame()
    for dd in d_rng:
        new_d = get_stats_for_day(dd, **kwargs)
        if new_d is None:
            continue
        else:
            df = df.append(new_d)

    return df

        