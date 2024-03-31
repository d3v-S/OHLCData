import pandas as pd
import glob
import os
from multiprocessing import Pool


DIR_DATA = "data\\"


def _getFiles(ticker="BNF", filepath=None):
    file_path = os.path.join(os.path.dirname(__file__), DIR_DATA) + "*{}.txt".format(ticker)
    if filepath is not None:
        file_path = filepath
    files =  glob.glob(file_path)
    return files

def _getDf(filename):
    print(f"reading file: {filename}")
    df = pd.read_csv(filename, sep=",",
                       names=["Ticker", "date", "time", "Open", "High", "Low", "Close"],
                       index_col=False, parse_dates=[["date", "time"]])
    
    # replace all 9:08 with 9:15:00
    mask = df['date_time'].dt.time == pd.to_datetime('09:08:00').time()
    df.loc[mask, 'date_time'] = df["date_time"].dt.normalize() + pd.Timedelta('09:15:00')
    
    mask = df['date_time'].dt.time == pd.to_datetime('09:09:00').time()
    df.loc[mask, 'date_time'] = df["date_time"].dt.normalize() + pd.Timedelta('09:15:00')
    
    return df

def _addDateAndTimeColumnsFromDateTimeIndex(df):
    try :
        df["datetime"] = pd.to_datetime(df.index, format="%Y%m%d")
        df["time"]     = df["datetime"].dt.time.astype(str)
        df["date"]     = df["datetime"].dt.date.astype(str)
    except:
        print ("Error in adding date and time columns")
        print(df)
    return df

def _makeUnifiedDf(filepath=None):
    df = pd.DataFrame()
    for file in _getFiles(filepath=filepath):
        df_tmp  = _getDf(file)
        df      = pd.concat([df, df_tmp])
    df = df.set_index("date_time")
    df = df.sort_index()
    df = _addDateAndTimeColumnsFromDateTimeIndex(df)
    return df

# multi-processing
def _makeUnifiedDf_MP(filepath=None):
    df = pd.DataFrame()
    dfs = []
    files = _getFiles(filepath=filepath)
    with Pool(8) as pool:
        dfs = pool.map(_getDf, files)
    for item in dfs:
        df      = df.append(item)
    df = df.set_index("date_time")
    df = df.sort_index()
    df = _addDateAndTimeColumnsFromDateTimeIndex(df)
    return df

def _makePerDayDataFromUnifiedDf(df):
    dfs = dict(tuple(df.groupby(df["date"])))
    return dfs

def _addMissing915ToPerDayDf(day_df):
    first_row = day_df.iloc[0].copy()
    if "9:16" in str(first_row["time"]):
        first_row["time"]           = "09:15:00"
        first_row["datetime"]       = first_row["datetime"].replace(minute=15)
        day_df.reset_index(drop=True, inplace=True)
        day_df                      = day_df.append(first_row)
        day_df["date_time"]         = day_df["datetime"]
        day_df.set_index("date_time", inplace=True)
        day_df.sort_index(inplace=True)
    return day_df

def _groupDataForTimeframe(df, timeframe):
    return  df.groupby(pd.Grouper(freq='{}Min'.format(timeframe), closed="right", label="right", origin="start")).agg({
    "Open": "first",
    "High": "max",
    "Low": "min",
    "Close": "last"})
    


## Public Apis ## 
class BnfOfflineDataSource:
    def __init__(self, filepath=None):
        self.unified_df = _makeUnifiedDf(filepath=filepath)
        self.dfs        = _makePerDayDataFromUnifiedDf(self.unified_df)
        self.dates      = list(self.dfs.keys())
    
    def getCompleteData(self)->pd.DataFrame:
        return self.unified_df
    
    def getDayData(self, date, timeframe=1):
        # each day data is shifted by 1 min or 1 tf to be correct, hence shift it by -1.
        if int(timeframe) == 1:
            df = _addMissing915ToPerDayDf(self.dfs[date]).shift(-1).dropna()  
        else:
            day_df = _addMissing915ToPerDayDf(self.dfs[date])
            df = _groupDataForTimeframe(day_df, timeframe=int(timeframe)).shift(-1).dropna()
        if "09:15:00" not in str(df.index[0]):
            print("This DF should be discarded: {}".format(str(df.index[0])))
        return df

    def getDates(self):
        return self.dates