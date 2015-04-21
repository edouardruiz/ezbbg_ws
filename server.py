# -*- coding: utf-8 -*-

import os
import json
import tempfile
import logging
import logging.config
import datetime as dt

import pandas as pd

from flask import Flask, jsonify, request, abort, Response

from ezbbg import bloomberg

__author__ = ('eruiz070210', 'dgaraud111714')

app = Flask(__name__)

HOST_DEBUG = 'localhost'
HOST = '0.0.0.0'
PORT = 5555
DATE_ISOFORMAT = "%Y-%m-%d"
DATETIME_ISOFORMAT = "%Y-%m-%dT%H:%M:%S"
DATETIME_MS_ISOFORMAT = "%Y-%m-%dT%H:%M:%S.%f"
LOG_DIR = os.path.join(tempfile.gettempdir(), 'ezbbg')
if not os.path.isdir(LOG_DIR):
    os.makedirs(LOG_DIR)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': "%(asctime)s: %(name)s: %(levelname)s: %(message)s"
        },
        'detailed': {
            'format': "%(asctime)s: %(name)s:%(levelname)s %(module)s:%(lineno)d: %(message)s"
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'simple',
            'stream': 'ext://sys.stdout'
        },
        'info_file_handler': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'simple',
            'filename': os.path.join(LOG_DIR, "info.log"),
            'maxBytes': '10485760',  # 10MB
            'backupCount': '20',
            'encoding': 'utf8'
        },
        'error_file_handler': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'ERROR',
            'formatter': 'simple',
            'filename': os.path.join(LOG_DIR, "errors.log"),
            'maxBytes': '10485760 # 10MB',
            'backupCount': '20',
            'encoding': 'utf8'
        }
    },
    'loggers': {
        'my_module': {
            'level': 'ERROR',
            'handlers': ['console'],
            'propagate': 'no'
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'info_file_handler', 'error_file_handler']
    }
}

logging.config.dictConfig(LOGGING)

def isoformat_date_converter(data):
    """Convert a string in ISO format into a date or a datetime.

      - date ISO format: YYYY-MM-DD
      - datetime ISO format: YYYY-MM-DDThh:mm:ss
      - datetime ISO format with microseonds: YYYY-MM-DDThh:mm:ss.ms

    According to the datetime factory, i.e. `datetime(2013,7,13,18,53)` or
    `datetime.now()`, it can be microseconds or not. Thus, the datetime ISO
    format can slightly differ.

    Parameters
    ----------

    data: str

    Return a date or a datetime according to the str input param.
    """
    try:
        timestamp = dt.datetime.strptime(data, DATE_ISOFORMAT)
        return dt.date(timestamp.year, timestamp.month, timestamp.day)
    except ValueError:
        # Try datetime ISO format with and without microseconds
        try:
            return dt.datetime.strptime(data, DATETIME_ISOFORMAT)
        except ValueError:
            return dt.datetime.strptime(data, DATETIME_MS_ISOFORMAT)

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj,'to_json'):
            return obj.to_json()
        if isinstance(obj, (dt.date, dt.datetime)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

# region Flask routes
@app.route('/reference_data', methods=['GET'])
def _server_get_reference_data():
    app.logger.info("Reference data query starting...")

    json_data = request.get_json()

    app.logger.info("Reference data query: %s", json_data)

    if json_data is None:
        abort(400)

    ticker_list = json_data.pop('ticker_list')
    field_list = json_data.pop('field_list')

    reference_data = bloomberg.get_reference_data(ticker_list, field_list, **json_data)

    app.logger.info("Reference data query ending")

    return Response(response=json.dumps(reference_data, cls=JSONEncoder),
                    status=200,
                    mimetype="application/json")

@app.route('/historical_data', methods=['GET'])
def _server_get_historical_data():
    app.logger.info("Historical data query starting...")
    json_data = request.get_json()
    app.logger.info("Historical data query: %s", json_data)

    if json_data is None:
        abort(400)

    ticker_list = json_data.pop('ticker_list')
    field_list = json_data.pop('field_list')
    start_date = json_data.pop('start_date')
    end_date = json_data.pop('end_date')
    start_date = isoformat_date_converter(start_date)
    end_date = isoformat_date_converter(end_date)

    data = bloomberg.get_historical_data(ticker_list, field_list,
                                         start_date, end_date,
                                         **json_data)
    app.logger.info("Historical data query ending")
    return Response(response=json.dumps(data, cls=JSONEncoder),
                    status=200,
                    mimetype="application/json")

@app.route('/version', methods=['GET'])
def _server_get_version():
    from ezbbg import __version__
    return __version__


@app.route('/fields_info', methods=['GET'])
def _server_get_fields_info():
    app.logger.info("Fields info query starting...")
    json_data = request.get_json()
    app.logger.info("Fields info query: %s", json_data)

    if json_data is None:
        abort(400)

    field_list = json_data.pop('field_list')
    return_field_documentation = json_data.pop('return_field_documentation', True)

    fields_info = bloomberg.get_fields_info(field_list, return_field_documentation)

    app.logger.info("Fields info query ending")

    return Response(response=json.dumps(fields_info, cls=JSONEncoder),
                    status=200,
                    mimetype="application/json")


@app.route('/fields', methods=['GET'])
def _server_search_fields():
    app.logger.info("Fields query starting...")
    json_data = request.get_json()
    app.logger.info("Fields query: %s", json_data)

    if json_data is None:
        abort(400)

    search_string = json_data.pop('search_string')
    return_field_documentation = json_data.pop('return_field_documentation', True)
    include_categories = json_data.pop('include_categories', None)
    include_product_type = json_data.pop('include_product_type', None)
    include_field_type = json_data.pop('include_field_type', None)
    exclude_categories = json_data.pop('exclude_categories', None)
    exclude_product_type = json_data.pop('exclude_product_type', None)
    exclude_field_type = json_data.pop('exclude_field_type', None)

    fields = bloomberg.search_fields(search_string,
                                     return_field_documentation,
                                     include_categories,
                                     include_product_type,
                                     include_field_type,
                                     exclude_categories,
                                     exclude_product_type,
                                     exclude_field_type)

    app.logger.info("Fields query ending")

    return Response(response=json.dumps(fields, cls=JSONEncoder),
                    status=200,
                    mimetype="application/json")


@app.route('/fields_by_category', methods=['GET'])
def _server_search_fields_by_category():
    app.logger.info("Fields by category query starting...")
    json_data = request.get_json()
    app.logger.info("Fields by category query: %s", json_data)

    if json_data is None:
        abort(400)

    search_string = json_data.pop('search_string')
    return_field_documentation = json_data.pop('return_field_documentation', True)
    exclude_categories = json_data.pop('exclude_categories', None)
    exclude_product_type = json_data.pop('exclude_product_type', None)
    exclude_field_type = json_data.pop('exclude_field_type', None)

    fields = bloomberg.search_fields_by_category(search_string,
                                                 return_field_documentation,
                                                 exclude_categories,
                                                 exclude_product_type,
                                                 exclude_field_type)

    app.logger.info("Fields by category query ending")

    return Response(response=json.dumps(fields, cls=JSONEncoder),
                    status=200,
                    mimetype="application/json")


def _test_get_reference_data():
    app.config['TESTING'] = True
    test_client = app.test_client()

    reference_data_request = {
        'ticker_list': ['SX5E Index', 'SPX Index'],
        'field_list': ['PX_LAST'],
        'tests': 'toto'
    }

    reference_data_request_js = json.dumps(reference_data_request)

    response = test_client.get('/reference_data', data=reference_data_request_js, content_type='application/json')

    res = json.loads(response.data)

    print(pd.read_json(res['SX5E Index']))


def _test_get_fields_info():
    app.config['TESTING'] = True
    test_client = app.test_client()

    fields_info_request = {
        'field_list': ['PX_LAST']
    }

    fields_request_js = json.dumps(fields_info_request)

    response = test_client.get('/fields_info', data=fields_request_js, content_type='application/json')

    res = json.loads(response.data)

    print(res['PX_LAST'])


def _test_search_fields_by_category():
    app.config['TESTING'] = True
    test_client = app.test_client()

    fields_info_request = {
        'search_string': 'last',
        'include_categories': ['Analysis'],
        'exclude_categories': ['Trading']
    }

    fields_by_category_request_js = json.dumps(fields_info_request)

    response = test_client.get('/fields_by_category', data=fields_by_category_request_js, content_type='application/json')

    res = json.loads(response.data)

    print(len(res))


def main():
    app.run(host=HOST, port=PORT, ssl_context='adhoc')

if __name__ == '__main__':
    # from sgilab.remote import remote_debug
    # if remote_debug('FR09263537D'):
    #     from ezbbg import bloomberg
    #     # _test_get_reference_data()
    #     # _test_get_fields_info()
    #     _test_search_fields_by_category()
    # app.run(host=HOST_DEBUG, port=PORT, ssl_context='adhoc', debug=True)
    main()
