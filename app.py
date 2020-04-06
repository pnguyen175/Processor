import connexion
import datetime
import yaml as yaml
from connexion import NoContent
import logging.config
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import json
from threading import Thread
from pykafka import KafkaClient
import pykafka
from flask_cors import CORS, cross_origin

sched = BackgroundScheduler()

with open ('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

def get_company_stat():
    logger.info('Request has started')

    try:
        with open(app_config['datastore']['filename'], 'r') as file:
            f = json.load(file)
            num_company = f['Num_Companies']
            num_order = f['Num_Orders']
            date = f['Date']
            context = {
                'num_companies': num_company,
                'num_total_orders': num_order,
                'timestamp': date
            }
            logger.debug(context)
        logger.info('Request has been completed')
        return context, 200
    except:
        logger.error('data.json does not exist')
        return 400

def populate_stats():
    '''Periodically update stats'''
    logger.info('Starting periodic processing')
    current_datetime = datetime.datetime.now()
    current = current_datetime.strftime("%Y-%m-%d:%H:%M:%S")

    try:
        f = open(app_config['datastore']['filename'])
    except:
        f = open(app_config['datastore']['filename'], 'w+')
        stats = {'Num_Companies': 0, 'Num_Orders': 0, 'Date': current}
        with open(app_config['datastore']['filename'], 'w') as file:
            json.dump(stats, file)
        print('JSON file not found. Creating new JSON file')

    with open(app_config['datastore']['filename'], 'r') as file:
            f = json.load(file)
            old_date = f['Date']

    headers = {
        "Content-type": "application/json"
    } 

    parameters = {
        "startDate": old_date,
        "endDate": current
    } 
    r = requests.get(app_config['eventstore']['url'] + '/info',
         params=parameters, headers=headers)

    if r.status_code != 200:
        logger.error('Could not get 200 response code')

    info_content = json.loads(r.content)
    info_count = len(info_content)

    r = requests.get(app_config['eventstore']['url'] + '/order',
         params=parameters, headers=headers)

    if r.status_code != 200:
        logger.error('Could not get 200 response code')

    order_content = json.loads(r.content)
    order_count = len(order_content)

    total_count = info_count + order_count

    logger.info('{0} events received!'.format(total_count))

    with open(app_config['datastore']['filename'], 'r') as file:
            f = json.load(file)
            old_date = f['Date']
            old_num_company = f['Num_Companies']
            old_num_order = f['Num_Orders']
            old_total_count = old_num_company + old_num_order

            new_info_count = info_count + old_num_company
            new_order_count = order_count + old_num_order 
            new_count = total_count + old_total_count
            logger.debug('{0} events received, events recieved is now {1} events since {2}'.format(total_count, new_count, old_date))

            file.close()
    with open(app_config['datastore']['filename'], 'w+') as file:
            new_json = {'Num_Companies': new_info_count, 'Num_Orders': new_order_count, 'Date': current}
            json.dump(new_json, file)
            file.close()

    logger.info('Periodic preocssing has ended')

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=app_config['scheduler']['period_sec'])
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml")
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'

logger = logging.getLogger('basicLogger')

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, use_reloader=False)