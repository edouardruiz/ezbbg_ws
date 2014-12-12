import json
import logging
import logging.config
import datetime as dt

import pandas as pd

from flask import Flask, jsonify, request, abort, Response

from ezbbg import bloomberg

__author__ = 'eruiz070210'

app = Flask(__name__)

HOST_DEBUG = 'localhost'
HOST = '0.0.0.0'
PORT = 5555
DATE_ISOFORMAT = "%Y-%m-%d"

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
            'filename': 'logs\info.log',
            'maxBytes': '10485760',  # 10MB
            'backupCount': '20',
            'encoding': 'utf8'
        },
        'error_file_handler': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'ERROR',
            'formatter': 'simple',
            'filename': 'logs\errors.log',
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


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj,'to_json'):
            return obj.to_json()
        return json.JSONEncoder.default(self, obj)


# region Flask routes
@app.route('/reference_data', methods=['GET'])
def _server_get_reference_data():
    app.logger.info("Reference data query starting...")

    json_data = request.get_json()

    app.logger.info("Reference data query: %s", json_data)

    if json_data is None:
        abort(400)

    ticker_list = json_data['ticker_list']
    field_list = json_data['field_list']

    del json_data['ticker_list']
    del json_data['field_list']

    reference_data = bloomberg.get_reference_data(ticker_list, field_list, **json_data)
    # reference_data = {'SX5E Index': pd.DataFrame({'PX_LAST': [1, 2, 3]}),
    #                   'SPX Index': pd.DataFrame({'PX_LAST': [4, 5, 6]})}

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
    start_date = dt.datetime.strptime(stard_date, DATE_ISOFORMAT)
    end_date = dt.datetime.strptime(end_date, DATE_ISOFORMAT)

    data = bloomberg.get_historical_data(ticker_list, field_list,
                                         start_date, end_date,
                                         **json_data)
    app.logger.info("Historical data query ending")
    return Response(response=json.dumps(data, cls=JSONEncoder),
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

def main():
    app.run(host=HOST, port=PORT)

if __name__ == '__main__':
    # _test_get_reference_data()
    #app.run(host=HOST_DEBUG, port=PORT, debug=True)
    main()
