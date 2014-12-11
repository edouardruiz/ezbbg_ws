# -*- coding: utf-8 -*-

import json

import requests

HOST = 'localhost'
PORT = 5555
HEADERS = {'Content-type': 'application/json', 'Accept': 'text/plain'}
URL_REFERENCE_DATA = "http://{0}:{1}/reference_data"

def get_reference_data(ticker_list, field_list, host=HOST, port=PORT, **kwargs):
    reference_data_request = {
        'ticker_list': ticker_list,
        'field_list': field_list
    }
    reference_data_request.update(kwargs)
    reference_data_request_js = json.dumps(reference_data_request)
    response = requests.get(URL_REFERENCE_DATA.format(host, port),
                            data=reference_data_request_js,
                            headers=HEADERS)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    host = 'FR09256841D'
    ticker_list = ['SX5E Index', 'SPX Index']
    field_list = ['PX_LAST']
    data = get_reference_data(ticker_list, field_list, host)
