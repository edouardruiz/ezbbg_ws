# -*- coding: utf-8 -*-

import json

import requests
import pandas as pd

__author__ = ('eruiz070210', 'dgaraud111714')

HOST = 'localhost'
PORT = 5555
HEADERS = {'Content-type': 'application/json', 'Accept': 'text/plain'}
# ComputerID:PORT or IP_address:PORT only via HTTPS
URL_EZBBG_ROOT = "https://{0}:{1}"
URL_REFERENCE_DATA = '/'.join([URL_EZBBG_ROOT, "reference_data"])
URL_HISTORICAL_DATA = '/'.join([URL_EZBBG_ROOT, "historical_data"])

def _refdata_converter(data):
    """Convert the deepest value of the JSON response to a DataFrame if it"s
    possible.
    """
    for ticker in data:
        for field, value in data[ticker].iteritems():
            if isinstance(value, basestring):
                try:
                    data[ticker][field] = pd.read_json(value)
                except ValueError:
                    pass
    return data

def get_reference_data(ticker_list, field_list, host=HOST, port=PORT, **kwargs):
    reference_data_request = {
        'ticker_list': ticker_list,
        'field_list': field_list
    }
    reference_data_request.update(kwargs)
    reference_data_request_js = json.dumps(reference_data_request)
    response = requests.get(URL_REFERENCE_DATA.format(host, port),
                            data=reference_data_request_js,
                            headers=HEADERS,
                            verify=False)
    response.raise_for_status()
    return _refdata_converter(response.json())

def get_historical_data(ticker_list, field_list, start_date, end_date,
                        host=HOST, port=PORT, **kwargs):
    historical_data_request = {
        'ticker_list': ticker_list,
        'field_list': field_list,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat()}
    historical_data_request.update(kwargs)
    historical_data_request_js = json.dumps(historical_data_request)
    response = requests.get(URL_HISTORICAL_DATA.format(host, port),
                            data=historical_data_request_js,
                            headers=HEADERS,
                            verify=False)
    response.raise_for_status()
    data_dict_json = response.json()
    return {k: pd.read_json(v) for k,v in data_dict_json.iteritems()}


if __name__ == "__main__":
    from datetime import date
    host = 'FR09256841D'
    #host = 'localhost'
    ticker_list = ['SX5E Index', 'SPX Index']
    field_list = ['PX_OPEN', 'PX_LAST']
    data = get_reference_data(ticker_list, field_list, host)
    start, end = date(2012, 1, 1), date(2014, 1, 1)
    histo_data = get_historical_data(ticker_list, field_list, start, end, host)
