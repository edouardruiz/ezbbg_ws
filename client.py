# -*- coding: utf-8 -*-

import json
import inspect
from functools import partial

import requests
import pandas as pd
# Disable 'Unverified HTTPS request' warning.
requests.packages.urllib3.disable_warnings()

__author__ = ('eruiz070210', 'dgaraud111714')
LOCAL_BBG = False

HOST = 'localhost'
PORT = 5555
HEADERS = {'Content-type': 'application/json', 'Accept': 'text/plain'}
# ComputerID:PORT or IP_address:PORT only via HTTPS
URL_EZBBG_ROOT = "https://{0}:{1}"
URL_REFERENCE_DATA = '/'.join([URL_EZBBG_ROOT, "reference_data"])
URL_HISTORICAL_DATA = '/'.join([URL_EZBBG_ROOT, "historical_data"])
URL_VERSION = '/'.join([URL_EZBBG_ROOT, "version"])

def _refdata_converter(data):
    """Convert the deepest value of the JSON response to a DataFrame if it's
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

def _get_server_version(host, port=PORT):
    """Get the version of ezbbg which runs on the server.
    """
    response = requests.get(URL_VERSION.format(host, port),
                            headers=HEADERS,
                            verify=False)
    response.raise_for_status()
    return response.content

def _get_reference_data(ticker_list, field_list, host, port=PORT, **kwargs):
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

def _get_historical_data(ticker_list, field_list, start_date, end_date,
                        host, port=PORT, **kwargs):
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
    if response.text == 'Error':
        return None
    data_dict_json = response.json()
    return {k: pd.read_json(v) for k,v in data_dict_json.iteritems()}

def update_host(host, port=PORT):
    """Update the (host, port) parameters for all HTTP client functions.
    """
    frame = inspect.currentframe()
    try:
        frame.f_globals["get_reference_data"] = partial(_get_reference_data,
                                                        host=host, port=port)
        frame.f_globals["get_historical_data"] = partial(_get_historical_data,
                                                         host=host, port=port)
        frame.f_globals["get_server_version"] = partial(_get_server_version,
                                                        host=host, port=port)
    finally:
        del frame


get_reference_data = partial(_get_reference_data, host=HOST, port=PORT)
get_historical_data = partial(_get_historical_data, host=HOST, port=PORT)
get_server_version = partial(_get_server_version, host=HOST, port=PORT)


if __name__ == "__main__":
    from datetime import date
    host = 'FR09256841D'
    #host = 'localhost'
    ticker_list = ['SX5E Index', 'SPX Index']
    field_list = ['PX_OPEN', 'PX_LAST']
    start, end = date(2012, 1, 1), date(2014, 1, 1)
    histo_data = get_historical_data(ticker_list, field_list, start, end, host)
    data = get_reference_data(ticker_list, field_list, host)
