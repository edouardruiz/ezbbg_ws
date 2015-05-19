# -*- coding: utf-8 -*-

import json
import inspect
from functools import partial

import requests
from dateutil import parser
import pandas as pd
# Disable 'Unverified HTTPS request' warning.
requests.packages.urllib3.disable_warnings()

__author__ = ('eruiz070210', 'dgaraud111714')
LOCAL_BBG = False

HOST = 'localhost'
PORT = 6666
HEADERS = {'Content-type': 'application/json', 'Accept': 'text/plain'}
# ComputerID:PORT or IP_address:PORT only via HTTPS
URL_EZBBG_ROOT = "https://{0}:{1}"
URL_REFERENCE_DATA = '/'.join([URL_EZBBG_ROOT, "reference_data"])
URL_HISTORICAL_DATA = '/'.join([URL_EZBBG_ROOT, "historical_data"])
URL_VERSION = '/'.join([URL_EZBBG_ROOT, "version"])
URL_FIELDS_INFO = '/'.join([URL_EZBBG_ROOT, "fields_info"])
URL_FIELDS = '/'.join([URL_EZBBG_ROOT, "fields"])
URL_FIELDS_BY_CATEGORY = '/'.join([URL_EZBBG_ROOT, "fields_by_category"])

def _refdata_converter(data):
    """Convert the deepest value of the JSON response to a DataFrame if it's
    possible.
    """
    for ticker in data:
        for field, value in data[ticker].iteritems():
            if isinstance(value, basestring):
                try:
                    data[ticker][field] = parser.parse(value).date()
                except (ValueError, TypeError):
                    pass
                try:
                    data[ticker][field] = pd.read_json(value).apply(
                        lambda x: pd.to_datetime(x) if x.dtypes == 'object' else x, axis=0)
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
        'ticker_list': [x for x in ticker_list],
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
        'ticker_list': [x for x in ticker_list],
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
    result = {k: pd.read_json(v) for k,v in data_dict_json.iteritems()}
    # Add the label 'date' to each index, as the returned DataFrame of the
    # original 'get_historical_data'.
    for k, df in result.iteritems():
        df.index.name = "date"
    return result

def _get_fields_info(field_list, host, port=PORT, return_field_documentation=True, **kwargs):
    fields_info_request = {
        'field_list': field_list,
        'return_field_documentation': return_field_documentation
    }
    fields_info_request.update(kwargs)
    fields_info_request_js = json.dumps(fields_info_request)
    response = requests.get(URL_FIELDS_INFO.format(host, port),
                            data=fields_info_request_js,
                            headers=HEADERS,
                            verify=False)
    response.raise_for_status()
    return response.json()

def _get_fields(search_string,
                host,
                port=PORT,
                return_field_documentation=True,
                include_categories=None,
                include_product_type=None,
                include_field_type=None,
                exclude_categories=None,
                exclude_product_type=None,
                exclude_field_type=None,
                **kwargs):
    fields_request = {
        'search_string': search_string,
        'return_field_documentation': return_field_documentation,
        'include_categories': include_categories,
        'include_product_type': include_product_type,
        'include_field_type': include_field_type,
        'exclude_categories': exclude_categories,
        'exclude_product_type': exclude_product_type,
        'exclude_field_type': exclude_field_type
    }
    fields_request.update(kwargs)
    fields_request_js = json.dumps(fields_request)
    response = requests.get(URL_FIELDS.format(host, port),
                            data=fields_request_js,
                            headers=HEADERS,
                            verify=False)
    response.raise_for_status()
    return response.json()

def _get_fields_by_category(search_string,
                            host,
                            port=PORT,
                            return_field_documentation=True,
                            exclude_categories=None,
                            exclude_product_type=None,
                            exclude_field_type=None,
                            **kwargs):
    fields_by_category_request = {
        'search_string': search_string,
        'return_field_documentation': return_field_documentation,
        'exclude_categories': exclude_categories,
        'exclude_product_type': exclude_product_type,
        'exclude_field_type': exclude_field_type
    }
    fields_by_category_request.update(kwargs)
    fields_by_category_request_js = json.dumps(fields_by_category_request)
    response = requests.get(URL_FIELDS_BY_CATEGORY.format(host, port),
                            data=fields_by_category_request_js,
                            headers=HEADERS,
                            verify=False)
    response.raise_for_status()
    return response.json()

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
        frame.f_globals["get_fields_info"] = partial(_get_fields_info,
                                                     host=host, port=port)
        frame.f_globals["get_fields"] = partial(_get_fields,
                                                host=host, port=port)
        frame.f_globals["get_fields_by_category"] = partial(_get_fields_by_category,
                                                            host=host, port=port)
    finally:
        del frame


get_reference_data = partial(_get_reference_data, host=HOST, port=PORT)
get_historical_data = partial(_get_historical_data, host=HOST, port=PORT)
get_server_version = partial(_get_server_version, host=HOST, port=PORT)
get_fields_info = partial(_get_fields_info, host=HOST, port=PORT)
get_fields = partial(_get_fields, host=HOST, port=PORT)
get_fields_by_category = partial(_get_fields_by_category, host=HOST, port=PORT)


if __name__ == "__main__":
    from datetime import date

    # host = 'FR09256841D'
    host = 'FR09263537D'
    #host = 'localhost'

    ticker_list = ['SX5E Index', 'SPX Index']
    field_list = ['PX_OPEN', 'PX_LAST']

    start, end = date(2012, 1, 1), date(2014, 1, 1)

    update_host(host)

    histo_data = get_historical_data(ticker_list, field_list, start, end)
    data = get_reference_data(ticker_list, field_list)

    fields_info = get_fields_info(field_list)

    search_string = 'earnings'

    fields_1 = get_fields(search_string, include_categories=["Ratings"])
    fields_2 = get_fields(search_string, include_categories=["Market"])

    fields_by_category = get_fields_by_category(search_string)
