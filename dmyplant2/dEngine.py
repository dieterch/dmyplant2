﻿from datetime import datetime, timedelta
import math
from pprint import pprint as pp
import pandas as pd
from dmyplant2.dMyplant import epoch_ts, mp_ts
import sys
import os
import pickle
import logging
import json
import arrow


class Engine(object):
    """ dmyplant Engine Class
        mp  .. MyPlant Object
        eng .. Pandas Validation Input DataFrame
    """
    _sn = 0
    _picklefile = ''
    _properties = {}
    _dataItems = {}
    _k = None
    _P = 0.0
    _d = {}

    def __init__(self, mp, eng):
        """ Engine Constructor
            load Instance from Pickle File or
            load Instance Data from Myplant
            if Myplant Cache Time is passed"""

        # take engine Myplant Serial Number from Validation Definition
        self._mp = mp
        self._sn = str(eng['serialNumber'])
        fname = os.getcwd() + '/data/' + self._sn
        self._picklefile = fname + '.pkl'    # load persitant data
        self._lastcontact = fname + '_lastcontact.pkl'
        try:
            with open(self._lastcontact, 'rb') as handle:
                self._last_fetch_date = pickle.load(handle)
        except:
            pass
        try:
            # fetch data from Myplant only on conditions below
            if self._cache_expired()['bool'] or (not os.path.exists(self._picklefile)):
                local_asset = self._mp.asset_data(self._sn)
                logging.debug(
                    f"{eng['Validation Engine']}, Engine Data fetched from Myplant")
                self.asset = self._restructure(local_asset)
                self._last_fetch_date = epoch_ts(datetime.now().timestamp())
            else:
                with open(self._picklefile, 'rb') as handle:
                    self.__dict__ = pickle.load(handle)
        except FileNotFoundError:
            logging.debug(
                f"{self._picklefile} not found, fetch Data from MyPlant Server")
        else:
            logging.debug(
                f"{__name__}: in cache mode, load data from {self._sn}.pkl")
        finally:
            logging.debug(
                f"Initialize Engine Object, SerialNumber: {self._sn}")
            self._d = self._engine_data(eng)
            self._set_oph_parameter()
            self._save()

    def __str__(self):
        return f"{self._sn} {self._d['Engine ID']} {self.Name[:20] + (self.Name[20:] and ' ..'):23s}"

    @property
    def time_since_last_server_contact(self):
        """
        get time since last Server contact
        """
        now = datetime.now().timestamp()
        delta = now - self.__dict__.get('_last_fetch_date', 0.0)
        return delta

    def _cache_expired(self):
        """
        time has since last Server contact
        returns (delta -> float, passed -> boolean)
        """
        delta = self.time_since_last_server_contact
        return {'delta': delta, 'bool': delta > self._mp.caching}

    def _restructure(self, local_asset):
        """
        Restructure Asset Data, add Variable
        Item names as dict key in dataItems & Properties
        """
        local_asset['properties'] = {
            p['name']: p for p in local_asset['properties']}
        local_asset['dataItems'] = {
            d['name']: d for d in local_asset['dataItems']}
        return local_asset

    def _set_oph_parameter(self):
        """
        internal
        Calculate line parameters, oph - line
        """
        self._k = float(self._d['oph parts']) / \
            (self._lastDataFlowDate - self._valstart_ts)

    def oph(self, ts):
        """
        linear inter- and extrapolation of oph(t)
        t -> epoch timestamp
        """
        y = self._k * (ts - self._valstart_ts)
        y = y if y > 0.0 else 0.0
        return y

    def _engine_data(self, eng) -> dict:
        """
        internal
        Extract basic Engine Data
        pd.DataFrame eng, Validation Definition
        """
        def calc_values(d) -> dict:
            oph_parts = float(d['Count_OpHour']) - float(d['oph@start'])
            d.update({'oph parts': oph_parts})
            return d

        dd = {}
        from_asset = {
            'nokey': ['serialNumber', 'status', 'id', 'model'],
            'properties': ['Engine Version', 'Engine Type', 'IB Unit Commissioning Date', 'Design Number',
                           'Engine ID', 'IB Control Software', 'IB Item Description Engine', 'IB Project Name'],
            'dataItems': ['Count_OpHour', 'Count_Start']}

        for key in from_asset:
            for ditem in from_asset[key]:
                dd[ditem] = self.get_data(key, ditem)

        dd['Name'] = eng['Validation Engine']
        self.Name = eng['Validation Engine']
        dd['P'] = int(str(dd['Engine Type'])[-2:])
        self._P = dd['P']
        dd['val start'] = eng['val start']
        dd['oph@start'] = eng['oph@start']
        # add calculated items
        dd = calc_values(dd)
        self._valstart_ts = epoch_ts(dd['val start'].timestamp())
        self._lastDataFlowDate = epoch_ts(dd['status'].get(
            'lastDataFlowDate', None))
        return dd

    def _save(self):
        """
        internal
        Persistant data storage to Pickle File
        """
        try:
            with open(self._lastcontact, 'wb') as handle:
                pickle.dump(self._last_fetch_date, handle, protocol=4)
        except FileNotFoundError:
            errortext = f'File {self._lastcontact} not found.'
            logging.error(errortext)
        try:
            with open(self._picklefile, 'wb') as handle:
                pickle.dump(self.__dict__, handle, protocol=4)
        except FileNotFoundError:
            errortext = f'File {self._picklefile} not found.'
            logging.error(errortext)
            # raise Exception(errortext)

    def get_data(self, key, item):
        """
        Get Item Value by Key, Item Name pair
        valid Myplant Keys are
        'nokey' data Item in Asset Date base structure
        'properties' data Item is in 'properties' list
        'dataItems' data Item is in 'dataItems' list

        e.g.: oph = e.get_data('dataItms','Count_OpHour')
        """
        return self.asset.get(item, None) if key == 'nokey' else self.asset[key].setdefault(item, {'value': None})['value']

    def get_property(self, item):
        """
        Get properties Item Value by Item Name

        e.g.: vers = e.get_property("Engine Version")
        """
        return self.get_data('properties', item)

    def get_dataItem(self, item):
        """
        Get  dataItems Item Value by Item Name

        e.g.: vers = e.get_dataItem("Monic_VoltCyl01")
        """
        return self.get_data('dataItems', item)

    def historical_dataItem(self, itemId, timestamp):
        """
        Get historical dataItem
        dataItemId  int64   Id of the DataItem to query.
        timestamp   int64   Optional,  timestamp in the DataItem history to query for.
        """
        try:
            res = self._mp.historical_dataItem(
                self.id, itemId, mp_ts(timestamp)).get('value', None)
        except:
            res = None
        return res

    def batch_hist_dataItem(self, itemId, p_from, p_to, timeCycle=3600):
        """
        Get np.array of dataItem history
        dataItemId  int64   Id of the DataItem to query.
        p_from      int64   timestamp start timestamp.
        p_to        int64   timestamp stop timestamp.
        timeCycle   int64   interval in seconds.
        """
        try:
            res = self._mp.history_dataItem(
                self.id, itemId, mp_ts(p_from), mp_ts(p_to), timeCycle)
            df = pd.DataFrame(res)
            df.columns = ['timestamp', str(itemId)]
            return df
        except:
            pass

    def batch_hist_dataItems(self, itemIds={161: 'CountOph'}, p_limit=None, p_from=None, p_to=None, timeCycle=86400,
                             assetType='J-Engine', includeMinMax='false', forceDownSampling='false'):
        """
        Get pandas dataFrame of dataItems history, either limit or From & to are required
        dataItemIds         dict   e.g. {161: 'CountOph'}, dict of dataItems to query.
        limit               int64, number of points to download
        p_from              string from iso date or timestamp,
        p_to                string stop iso date or timestamp.
        timeCycle           int64  interval in seconds.
        assetType           string default 'J-Engine' 
        includeMinMax       string 'false' 
        forceDownSampling   string 'false'
        """
        try:
            tt = r""
            if p_limit:
                tt = r"&limit=" + str(p_limit)
            else:
                if p_from and p_to:
                    tt = r'&from=' + str(arrow.get(p_from).timestamp * 1000) + \
                        r'&to=' + str(arrow.get(p_to).timestamp * 1000)
                else:
                    raise Exception(
                        r"batch_hist_dataItems, invalid Parameters")

            tdef = itemIds
            tdj = ','.join([str(s) for s in tdef.keys()])

            ttimecycle = timeCycle
            tassetType = assetType
            tincludeMinMax = includeMinMax
            tforceDownSampling = forceDownSampling

            url = r'/asset/' + str(self.id) + \
                r'/history/batchdata' + \
                r'?assetType=' + str(tassetType) + \
                tt + \
                r'&dataItemIds=' + str(tdj) + \
                r'&timeCycle=' + str(ttimecycle) + \
                r'&includeMinMax=' + str(tincludeMinMax) + \
                r'&forceDownSampling=' + str(tforceDownSampling)

            # fetch data from myplant ....
            data = self._mp.fetchdata(url)

            # restructure data to dict
            ds = dict()
            ds['labels'] = ['time'] + [tdef[x] for x in data['columns'][1]]
            ds['data'] = [[r[0]] + [rr[0] for rr in r[1]]
                          for r in data['data']]

            # import to Pandas DataFrame
            df = pd.DataFrame(ds['data'], columns=ds['labels'])
            return df
        except:
            return None

    def batch_hist_alarms(self, p_severities=[600, 800], p_offset=0, p_limit=None, p_from=None, p_to=None):
        """
        Get pandas dataFrame of Events history, either limit or From & to are required
        p_severities          list   600,650 ... operational messages
                                   700 ... warnings
                                   800 ... alarms
        p_offset            int64, number of messages to skip 
        p_limit             int64, number of messages to download
        p_from              string timestamp in milliseconds.
        p_to                string timestamp in milliseconds.
        """
        try:
            tt = r""
            if p_limit:
                tt = r"&offset=" + str(p_offset) + \
                    r"&limit=" + str(p_limit)
            else:
                if p_from and p_to:
                    tt = r'&from=' + str(arrow.get(p_from).timestamp * 1000) + \
                        r'&to=' + str(arrow.get(p_to).timestamp * 1000)
                else:
                    raise Exception(
                        r"batch_hist_alarms, invalid Parameters")

            tsvj = ','.join([str(s) for s in p_severities])

            url = r'/asset/' + str(self.id) + \
                r'/history/alarms' + \
                r'?severities=' + str(tsvj) + tt

            # fetch messages from myplant ....
            messages = self._mp.fetchdata(url)

            # import to Pandas DataFrame
            dm = pd.DataFrame(messages)
            return dm
        except:
            return None

    @ property
    def id(self):
        """
        MyPlant Asset id

        e.g.: id = e.id
        """
        return self._d['id']

    @ property
    def serialNumber(self):
        """
        MyPlant serialNumber
        e.g.: serialNumber = e.serialNumber
        """
        return self._d['serialNumber']

    @ property
    def P(self):
        """
        Number of Parts
        e.g.: m = e.P
        """
        return self._P

    @ property
    def properties(self):
        """
        properties dict
        e.g.: prop = e.properties
        """
        return self.asset['properties']

    @ property
    def dataItems(self):
        """
        dataItems dict
        e.g.: dataItems = e.dataItems
        """
        return self.asset['dataItems']

    @ property
    def valstart_ts(self):
        """
        Individual Validation Start Date
        as EPOCH timestamp
        e.g.: vs = e.valstart_ts
        """
        return self._valstart_ts

    @ property
    def valstart_oph(self):
        """
        Individual Validation Start Date
        as EPOCH timestamp
        e.g.: vs = e.valstart_ts
        """
        return self._d['oph@start']

    @ property
    def now_ts(self):
        """
        Actual Date & Time
        as EPOCH timestamp
        e.g.: now = e.now_ts
        """
        return datetime.now().timestamp()

    @ property
    def dash(self):
        dashcols = [
            'Name',
            'Engine ID',
            'Design Number',
            'Engine Type',
            'Engine Version',
            'P',
            'P_NOM'
            'BMEP',
            'serialNumber',
            'id',
            'Count_OpHour',
            'val start',
            'oph@start',
            'oph parts',
        ]
        # iterate over engines and columns
        ldash = [self._d[c] for c in dashcols]
        return ldash


class EngineReadOnly(Engine):
    """
    Inherited Read Only Engine Object
    Constructor uses SerialNumber
    e.g.: e = EngineReadOnly('1386177')
    """

    def __init__(self, sn):
        """
        ReadOnly Engine Constructor
        load Instance from Engine Pickle File
        """
        self._sn = str(sn)
        self._picklefile = os.getcwd() + '/data/' + self._sn + '.pkl'
        try:
            with open(self._picklefile, 'rb') as handle:
                self.__dict__ = pickle.load(handle)
        except FileNotFoundError:
            logging.debug(f"{self._picklefile} not found.")


if __name__ == '__main__':
    import traceback
    import dmyplant2

    try:
        e = EngineReadOnly('1386177')
        # with open(os.getcwd() + '/data/' + e.serialNumber + '.json', 'w') as fp:
        #    json.dump(e.asset, fp)
        print(e.dash)

    except Exception as ex:
        print(ex)
        traceback.print_tb(ex.__traceback__)
