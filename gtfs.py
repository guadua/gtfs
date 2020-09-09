#!/usr/bin/env python3 
import io, os
import time
import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2
from configparser import ConfigParser

from pdb import set_trace

def statistics(config):
    url = config['STATISTICS']['url']
    req = requests.get(url)
    df = pd.read_csv(io.StringIO(req.content.decode('utf8')))

    return df

def write_stats(df):
    df = df[df['rt_id'] == df['rt_id']].sort_values('rt_id')[[col for col in df.columns if col.startswith('rt')]].drop_duplicates()
    # set_trace()
    inifile = 'gtfs_rt.ini'
    index_col = 'rt_id'
    if os.path.exists(inifile):
        print('%s already exists.' % inifile)
    else:
        with open(inifile, 'w') as f:
            for index, row in df.iterrows():
                f.write('[%s]\n' % row[index_col])
                for col in row.index:
                    if col.startswith('rt'):
                        f.write('%s=%s\n' % (col, row[col]))
        print('wrote to %s.' % inifile)


def fetch(config):
    url = config['PB']['url']
    print(url)

    for i in range(100):
        time.sleep(1)
        fetch(url)

        feed = gtfs_realtime_pb2.FeedMessage()
        req = requests.get(url)
        feed.ParseFromString(req.content)
        # print(feed)
        for entity in feed.entity:
            # if entity.HasField('trip_update'):
            #     print(entity.trip_update)
            elapsed = (time.time()-entity.vehicle.timestamp)
            print('%s updated %.04f secs ago.' % (entity.id, elapsed))
            print(entity.vehicle.position)

def main():    
    config = ConfigParser()
    config.read('setting.ini')

    df = statistics(config)
    write_stats(df)
    #fetch(config)

if __name__ == '__main__':
    main()