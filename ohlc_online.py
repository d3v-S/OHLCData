
import os
import math, time
import requests
import  requests_cache
from bs4 import BeautifulSoup as bs
import pandas as pd
import dill as pickle



### Exceptions:

class DownloadFailedException(Exception):
    pass

class IndexNotFoundException(Exception):
    pass

class StockNotFoundException(Exception):
    pass

class DataFormatException(Exception):
    pass

# download is okay, but data statuscode is not okay
class DownloadedDataException(Exception):
    pass

# date range exception, if date range is beyond something
class DateRangeException(Exception):
    pass

# probably request was malformed. 
class RequestingParamException(Exception):
    pass

# instrument key not found
class InstrumentKeyNotFoundException(Exception):
    pass

# public key: 
class DatasourceNotAvailableException(Exception):
    pass


### Helpers:
class Helper:
    default_df_columns = ["epoch", "Open", "High", "Low", "Close", "Volume"]
    logging            = True
    datetime_format    = "%Y-%m-%d %H:%M"
    
    @staticmethod
    def log(string):
        if Helper.logging:
            print(string)
    
    
    @staticmethod
    def getUrl(url: str, headers=None, timeout=20):
        Helper.log(f"getting url: {url}")
        try:
            if not headers:
                res = requests.get(url, timeout=timeout)
            else:
                res = requests.get(url, headers=headers, timeout=timeout)
            if res.status_code == 200:
                return res
            raise RequestingParamException() # other status codes.
        except:
            raise DownloadFailedException()
    
    @staticmethod
    def getCachedUrl(url):
        session = requests_cache.CachedSession('mc_index_cache')
        return session.get(url)
    
    @staticmethod
    def getCachedSoup(url: str):
        res = Helper.getCachedUrl(url)
        return bs(res.text, 'html.parser')

    @staticmethod
    def officialNamesOfIndex(name: str):
        if name.upper() == "BANKNIFTY" or name.lower() == "banknifty":
            return "NIFTY BANK"
        if name.upper() == "NIFTY" or name.lower() == "nifty":
            return "NIFTY 50"
        if name.upper() == "MIDCAP" or name.lower() == "midcap":
            return "NIFTY MIDCAP 50"
        return name 
    
    @staticmethod
    def jsonTypeAtoDf(jsons, input_cols=["t", "o", "h", "l", "c", "v"]):
        """ 
        input = {    s: ok, t: [], o: [], h: [], l: [], c: [], v: [] }
        t: should be in epochs
        """
        try:
            df              = pd.DataFrame(list(zip(jsons[input_cols[0]], jsons[input_cols[1]], jsons[input_cols[2]], jsons[input_cols[3]], jsons[input_cols[4]], jsons[input_cols[5]])), 
                                        columns=Helper.default_df_columns)
            df["date"]      = pd.to_datetime(df["epoch"], unit="s") + pd.Timedelta("05:30:00")
            df["date_time"] = df["date"]
            df              = df.set_index("date_time")
            return df.dropna()
        except:
            raise DataFormatException()
    
    @staticmethod
    def listOfListsToDf(lol, input_cols=["Time", "Open", "High", "Low", "Close", "Volume", "OI"]):
        """ dataformat: [[time, o, h, l, c, v, oi], [], []]"""
        df              = pd.DataFrame(lol, columns = input_cols)
        df["date_time"] = pd.to_datetime(df["Time"]).dt.tz_localize(None)
        df              = df.set_index("date_time")
        df.sort_values(by="date_time" , inplace=True)
        return df

    @staticmethod    
    def groupDataForTimeframe(df: pd.DataFrame, timeframe: int) -> pd.DataFrame:
        """
        for a datetime indexed dataframe of OHLCV, it groups them based on N minutes, where N is timeframe.
        Args:
            df (pd.DataFrame): 
            timeframe (int)  : 
        Returns:
            pd.DataFrame: grouped dataframe
        """
        return  df.groupby(pd.Grouper(freq='{}Min'.format(timeframe), closed="right", label="right", origin="start")).agg({
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last", 
        "Volume": "sum"})  
        
    @staticmethod
    def removeRowsBeforeDate(df: pd.DataFrame, date):
        """
        This is required for countback. We are not sure of countback, hence download everything
        and remove whatever is before "start" date.
        """
        df['date'] = pd.to_datetime(df['date'])
        return df[~(df['date'] < date)]
    
    @staticmethod
    def incrementDateInString(date, delta=1):
        from datetime import datetime, timedelta
        date = datetime.strptime(date, "%Y-%m-%d")
        modified_date = date + timedelta(days=delta)
        return datetime.strftime(modified_date, "%Y-%m-%d")
    
    
    @staticmethod
    def removeRowsAfterDate(df: pd.DataFrame, date):
        """
        This is required for countback. We are not sure of countback, hence download everything
        and remove whatever is before "start" date and also after merging, remove anything after 
        "end" date
        """
        Helper.log(f"Removing dates after {date}")
        df['date'] = pd.to_datetime(df.index)
        # for some reason, it includes the exact date too, hence increment the date.
        return df[~(df['date'] > Helper.incrementDateInString(date, 1))]
        
    
    # critical dataframe information
    # check the data, how it behaves.
    # grouping directly here does something like this: example 3 min,
    # 9:15 data remains same, 9:18 -> [9:16, 9:17, 9:18]
    # but it should have been: 9:15 -> [9:15, 9:16, 9:17]
    # so, shift it down by one and fill it with zero and once grouped, again shift it left:
    @staticmethod
    def getGroupedDf(df, tf):
        if tf == 1:
            return df
        fixed_df = df.shift(1).fillna(0)
        final_df = Helper.groupDataForTimeframe(fixed_df, int(tf)).shift(-1).dropna()
        return final_df
    
    # splits a complete dataframe into per day basis
    # return dict [date] = dataframe    
    @staticmethod
    def mapDfToPerDay(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """ 
        convert yearly data into per day data.
        Much more efficient than downloading each day data, everytime
        Returns:
            date_df_map (dict_) : key:: date, val:: dataframe
        """
        date_df_map          = {}
        df["tmp"]            = df.index
        df[["date", "time"]] = df["tmp"].astype(str).str.split(expand=True)
        df_1                 = df.groupby(["date"])
        for date in df_1.groups:
            new_df = df_1.get_group(date)
            # for some unknown reason, for given tf, example 5,
            # data starts from 9:10
            # this 9:10 data is basically yesterday's last data.
            # drop first row:
            time_row1 = str(list(new_df.index)[0])
            if "9:15" not in time_row1:
                new_df = new_df.iloc[1:]
            date_df_map[date] = new_df
        return date_df_map
    
    @staticmethod
    def genPath(symbol, start, end):
        return f"{symbol}_{start}_{end}.pkl"

    @staticmethod
    def save(path, data):
        with open(path, 'wb') as fp:
            pickle.dump(data, fp)

    @staticmethod
    def saveTxt(path, data):
        with open(path, 'a+') as fp:
            fp.write(data)

    @staticmethod
    def load(path):
        with open(path, "rb") as fp:
            return pickle.load(fp)

    @staticmethod
    def getEpochFromDateTime(date, time_):
        return math.trunc(time.mktime(time.strptime(f"{date} {time_}", Helper.datetime_format)))

    @staticmethod
    def getEpochStart(date):
        return Helper.getEpochFromDateTime(date, "9:15")
    
    @staticmethod
    def getEpochEnd(date):
        return Helper.getEpochFromDateTime(date, "15:30")
    
    @staticmethod
    def genApproxCountbackFromEpoch(start_epoch, end_epoch):
        if (end_epoch - start_epoch) < 86400:
            diff_days = int((end_epoch - start_epoch) / 60)  # these many minutes elapsed between then, hence countback = 1 min candles;
            return diff_days
        else:
            diff_days     = int((end_epoch - start_epoch) / 86400) # days elapsed; each day has 375 1 min candles
        countback     = diff_days * 377   # days with shorter trading session will have less number of candles, so we can have more candles, never less
        return countback
    
    @staticmethod
    def getPathWhereThisScriptIsExecuting(filename):
        directory = os.path.dirname(__file__)  # wherever this file resides, look into that directory
        return os.path.join(directory, filename)
    
    


class Downloader:
    def __init__(self, symbol, start, end, cached=False):
        self.symbol = symbol
        self.start  = start
        self.end    = end
        self.cached = cached
        self.path   = Helper.genPath(symbol, start, end)
        self._initData()
        
    def _loadData(self):
        self.data = Helper.load(self.path)
    
    def _saveData(self):
        Helper.save(self.path, self.data)
    
    def _initData(self):
        raise NotImplementedError
        
    def df(self, tf):
        raise NotImplementedError





### Downloaders

## 
# Economic Times
##

class ETHelper:
    @staticmethod
    def getUrl(url: str) -> dict:
        res = Helper.getUrl(url)
        json = res.json()
        if json["s"] != "ok":
            raise DownloadedDataException()
        if json["noData"]:
            raise DateRangeException()
        return json
    
    @staticmethod
    def genUrl(type_,symbol, start, end, tf=1):
        """ has same url for index and stocks.
            countsback from end to start.
            type = index, stock
        """
        base_url = "https://etelection.indiatimes.com/ET_Charts/india-market/{type}/history?symbol={symbol}&resolution={timeframe}&to={end}&countback={countback}&currencyCode=INR"
        start_epoch = Helper.getEpochStart(start)
        end_epoch = Helper.getEpochEnd(end)
        countback = Helper.genApproxCountbackFromEpoch(start_epoch, end_epoch)
        return base_url.format(type=type_, symbol=symbol, timeframe=tf, end=end_epoch, countback=countback)

    @staticmethod
    def genIndexUrl(symbol, start, end, tf=1):
        return ETHelper.genUrl("index", symbol, start, end, tf=tf)
    
    @staticmethod
    def genStockUrl(symbol, start, end, tf=1):
        return ETHelper.genUrl("stock", symbol, start, end, tf=tf)

    @staticmethod
    def download(symbol, start, end, tf=1):
        """ stocks have EQ in their name in ET"""
        try:
            url = ETHelper.genIndexUrl(symbol, start, end, tf=tf)
            return ETHelper.getUrl(url)
        except:
            url = ETHelper.genStockUrl(f"{symbol}EQ", start, end, tf=tf)
            return ETHelper.getUrl(url)


class ET(Downloader):
    def __init__(self, symbol, start, end, cached=False):
        super().__init__(symbol, start, end, cached)
        
    def _initData(self):
        try:
            if self.cached:
                self._loadData()
            else:
                raise Exception()
        except:
            self.data   = self.__download()   # download once, for other TF we can calculate from the downloaded data
            self._saveData()
            
    def __download(self):
        return ETHelper.download(self.symbol, self.start, self.end, 1)
    
    def df(self, tf):
        tf = int(tf)
        df   = Helper.jsonTypeAtoDf(self.data)
        df   = Helper.removeRowsBeforeDate(df, self.start)
        if tf == 1:
            return df
        else:
            return Helper.removeRowsAfterDate(Helper.getGroupedDf(df, tf), self.end)
        
    
##
# MONEYCONTROL
##

class MCHelper:
    datetime_format = "%Y-%m-%d %H:%M"
    headers = {
            "Host": "priceapi.moneycontrol.com",
            "User-Agent"               : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Accept"                   : "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language"          : "en-US,en;q=0.5",
            "Accept-Encoding"          : "gzip, deflate, br",
            "Connection"               : "keep-alive",
            "DNT"                      : "1",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest"           : "document",
            "Sec-Fetch-Mode"           : "navigate",
            "Sec-Fetch-Site"           : "none",
            "Sec-Fetch-User"           : "?1",
            "Sec-GPC"                  : "1"
            }
    @staticmethod
    def getIndexCodeMap():
        """
        Moneycontrol has codes for indexes.
        From the said url, this extracts the indexes and the corresponding code.
        
        Returns:
            Dict[str, str]: _description_
        """
        url            = "https://www.moneycontrol.com/markets/indian-indices/"
        soup           = Helper.getCachedSoup(url)
        classes        = soup.find_all(class_="indicesList")
        index_code_map = {}
        for class_ in classes:
            index_name = class_["data-name"].upper()
            index_code = class_["data-subid"]
            
            index_code_map[index_name] = index_code
            index_code_map[index_name.lower()] = index_code
        return index_code_map
    
    @staticmethod
    def getCodeForIndex(index):
        try:
            if index.upper() == "FINNIFTY":
                return 47      # not in indices, hence hardcoded
            if index.upper() == "BANKNIFTY":
                return 23
            if index.upper() == "MIDCAP":
                return 27
            index_name = Helper.officialNamesOfIndex(index)
            return MCHelper.getIndexCodeMap()[index_name]
        except Exception as e:
            print("Error : {}".format(e))
            raise IndexNotFoundException()

    @staticmethod
    def getUrl(url: str) -> dict:
        res  = Helper.getUrl(url, headers=MCHelper.headers)
        json = res.json()
        if "error" in json["s"]:
            raise DownloadedDataException()
        if json["s"] == "no_data":
            raise DateRangeException()
        return json

    @staticmethod
    def genStockUrl(symbol: str, start: str, end: str) -> str:
        # will always be less than or equal to "start". Fix it while making Dataframe, drop dates before start.
        """
        url = https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history?symbol={symbol}&resolution={timeframe}&from={start}&to={end}&countback=1&currencyCode=INR
        Difference with index url: it gives number of countback candle OHLC from "to" with the given "resolution". 
        """
        base_url    = "https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history?symbol={symbol}&resolution={timeframe}&to={end}&countback={countback}"
        start_epoch = Helper.getEpochStart(start) #math.trunc(time.mktime(time.strptime(str(start) + " 9:15", MCHelper.datetime_format)))
        end_epoch   = Helper.getEpochEnd(end) #math.trunc(time.mktime(time.strptime(str(end) + " 15:30", MCHelper.datetime_format)))
        countback   = Helper.genApproxCountbackFromEpoch(start_epoch, end_epoch)
        url         = base_url.format(symbol=symbol, end=end_epoch, timeframe="1", countback=countback)
        return url

    @staticmethod
    def genIndexUrl(symbol: str, start: str, end: str) -> str:
        # latest moneycontrol has different symbol but still works with previous version of sending index code.
        """
        url = https://priceapi.moneycontrol.com//techCharts/indianMarket/index/history?symbol=in%3BNSX&resolution=5&from=1698855106&to=1698858106&countback=2&currencyCode=INR
        in%3BNSX :: new version, but works with code too. hence we are using the code, like 31, 23 etc from index_code_map
        """
        start_epoch = Helper.getEpochStart(start) #math.trunc(time.mktime(time.strptime(str(start) + " 9:15", MCHelper.datetime_format)))
        end_epoch   = Helper.getEpochEnd(end) #math.trunc(time.mktime(time.strptime(str(end) + " 15:30", MCHelper.datetime_format)))
        return MCHelper.genIndexUrlUsingEpoch(symbol, start_epoch, end_epoch)

    @staticmethod
    def genIndexUrlUsingEpoch(symbol: str, start_epoch: str, end_epoch: str) -> str:
        # latest moneycontrol has different symbol but still works with previous version of sending index code. 
        #? Figure out how the new symbols are generated.
        """
        url = https://priceapi.moneycontrol.com//techCharts/indianMarket/index/history?symbol=in%3BNSX&resolution=5&from=1698855106&to=1698858106&countback=2&currencyCode=INR
        in%3BNSX :: new version, but works with code too. hence we are using the code, like 31, 23 etc from index_code_map
        """
        base_url = "https://priceapi.moneycontrol.com/techCharts/history?symbol={symbol}&resolution={timeframe}&from={start}&to={end}"
        code     = MCHelper.getCodeForIndex(symbol)
        tf       = "1"  #* timeframe is always 1 because we merge to create all timeframe in dataframes
        url      = base_url.format(timeframe = tf, start=start_epoch, end=end_epoch, symbol=code)
        return url

    @staticmethod
    def downloadIndex(symbol, start, end)->dict:
        """
        if the timestamps are in future, json's status code will be "no_data" and timestamp of last available data.
        in that case, move back 24 hours from the last available data timestamp and re-run download.
        # not requried for Stocks. that works by countback, hence for future date will give the countbacks from last date.
        json ["s"] 
        ==    "ok"      :: data is ok and we can proceed.
        ==    "no_data" :: maybe we are in future and "nextTime" key gives the last data point available.
        """
        url = MCHelper.genIndexUrl(symbol, start, end)
        try:
            return MCHelper.getUrl(url)
        except DateRangeException:
            res               = Helper.getUrl(url, MCHelper.headers)
            json              = res.json()
            fixed_end_epoch   = int(json["nextTime"])
            fixed_start_epoch = fixed_end_epoch - (24 * 60 * 60 - 1)  #* at this point, we are moving a day back from the last data point, just a second less.
            url               = MCHelper.genIndexUrlUsingEpoch(symbol, fixed_start_epoch, fixed_end_epoch)
            return MCHelper.getUrl(url)

    @staticmethod
    def downloadStock(symbol, start, end)->dict:
        url = MCHelper.genStockUrl(symbol, start, end)
        return MCHelper.getUrl(url)
    

class MC(Downloader):
    def __init__(self, symbol, start, end, cached=False):
        super().__init__(symbol, start, end, cached)
        
    def _initData(self):
        try:
            if self.cached:
                self._loadData()
            else:
                raise Exception()
        except:
            self.data   = self.__download()   # download once, for other TF we can calculate from the downloaded data
            self._saveData()
            
    def __download(self):
        try:
            return MCHelper.downloadIndex(self.symbol, self.start, self.end)
        except IndexNotFoundException:
            return MCHelper.downloadStock(self.symbol, self.start, self.end)

    def df(self, tf):
        tf = int(tf)
        df   = Helper.jsonTypeAtoDf(self.data)
        df   = Helper.removeRowsBeforeDate(df, self.start)
        if tf == 1:
            return df
        else:
            return Helper.getGroupedDf(df, tf)



##
# UPSTOX
##

class UpstoxHelper:
    nse_instruments = None
    bse_instruments = None
    
    @staticmethod
    def getUrl(url):
        headers = {
            'Accept': 'application/json'
        }
        res             = Helper.getUrl(url, headers=headers)
        print(res)
        json            = res.json()
        if json["status"] != "success":
            raise DownloadedDataException()
        return json["data"]["candles"]
    
    @staticmethod
    def readInstrumentsFile(filepath) -> pd.DataFrame:
        return pd.read_csv(Helper.getPathWhereThisScriptIsExecuting(filepath), sep=",", engine='python')
    
    @staticmethod
    def getInstrumentKeyFromDataframe(df, symbol):
        val =  list(df[df["name"].str.contains(symbol, na=False, case=False)]["instrument_key"].astype('string'))
        if not val:
            val =  list(df[df["tradingsymbol"].str.contains(symbol, na=False, case=False)]["instrument_key"].astype('string'))
            if not val:
                val = ["None"]
        return val

    @staticmethod
    def removeArtifactsFromInstrumentKey(instrument_key):
        return instrument_key.replace(" ", "%20").replace("|", "%7C")
    
    @staticmethod
    def getNseInstrument(symbol):
        if UpstoxHelper.nse_instruments is None:
            UpstoxHelper.nse_instruments = UpstoxHelper.readInstrumentsFile("./instrument_data/NSE.csv") 
            instrument_key = UpstoxHelper.getInstrumentKeyFromDataframe(UpstoxHelper.nse_instruments, symbol)[0]
        return UpstoxHelper.removeArtifactsFromInstrumentKey(instrument_key)
    
    @staticmethod
    def getBseInstrument(symbol):
        if UpstoxHelper.bse_instruments is None:
            UpstoxHelper.bse_instruments = UpstoxHelper.readInstrumentsFile("./instrument_data/BSE.csv") 
            instrument_key = UpstoxHelper.getInstrumentKeyFromDataframe(UpstoxHelper.bse_instruments, symbol)[0]
        return UpstoxHelper.removeArtifactsFromInstrumentKey(instrument_key)
    
    @staticmethod
    def getInstrumentKey(symbol):
        inst_key = UpstoxHelper.getNseInstrument(symbol)
        Helper.log(f"nse_instrument_key: {inst_key}")
        if inst_key is None:
            inst_key = UpstoxHelper.getBseInstrument(symbol)
        if inst_key is None:
            raise InstrumentKeyNotFoundException()
        Helper.log(f"bse_instrument_key: {inst_key}")
        return inst_key
            
    
    @staticmethod
    def genInstrumentKeyUrl(instrument_key, start, end, tf):
        base_url = "https://api.upstox.com/v2/historical-candle/{instrument_key}/{tf}/{end}/{start}"
        return base_url.format(instrument_key=instrument_key, tf=tf, end=end, start=start)

    @staticmethod
    def genUrl(symbol, start, end, tf):
        inst_key = UpstoxHelper.getInstrumentKey(symbol)
        return UpstoxHelper.genInstrumentKeyUrl(inst_key, start, end, tf)    
    
    @staticmethod
    def officialNamesOfIndex(name: str):
        if name.upper() == "BANKNIFTY" or name.lower() == "banknifty":
            return "Nifty BANK"
        if name.upper() == "NIFTY" or name.lower() == "nifty":
            return "Nifty 50"
        if name.upper() == "MIDCAP" or name.lower() == "midcap":
            return "NIFTY MIDCAP SELECT"
        return name 

class Upstox:
    def __init__(self, symbol, start, end):
        self.symbol = UpstoxHelper.officialNamesOfIndex(symbol)
        self.start  = start
        self.end    = end
    
    def __download(self, tf):
        url = UpstoxHelper.genUrl(self.symbol, self.start, self.end, tf)
        return UpstoxHelper.getUrl(url)
    
    def __df(self, tf):
        data = self.__download(tf)
        return Helper.listOfListsToDf(data)
    
    def dfDaily(self):
        return self.__df("day")
    
    def dfWeekly(self):
        return self.__df("week")

    def dfMonthly(self):
        return self.__df("month")
    
    def df(self, tf): # per minute
        if tf == 1:
            return self.__df("1minute")
        else:
            return Helper.getGroupedDf(self.__df("1minute"), tf)


##
# PUBLIC METHODS / API:
##
class HistoricalData:
    DATASOURCE = "MC"
    def __init__(self, symbol, start, end):
        self.datasource_obj = self.__initDatasourceObj(symbol, start, end)

    def currentDatasource(self):
        return HistoricalData.DATASOURCE
    
    def allDatasources(self):
        return ["MC", "ET", "Upstox"]
    
    def setDatasource(self, datasource):
        HistoricalData.DATASOURCE = datasource
    
    def __initDatasourceObj(self, symbol, start, end):
        if HistoricalData.DATASOURCE == "MC":
            return MC(symbol, start, end)
        elif HistoricalData.DATASOURCE == "ET":
            return ET(symbol, start, end)
        elif HistoricalData.DATASOURCE == "Upstox":
            return Upstox(symbol, start, end)
        else:
            print(".allDatasources for list of datasources supported")
            raise DatasourceNotAvailableException()
    
    def df(self, tf):
        return self.datasource_obj.df(tf)

    def dfForDate(self, date, tf):
        """ returns: dict_ [date] = dataframe"""
        return Helper.mapDfToPerDay(self.df(tf))[date]
