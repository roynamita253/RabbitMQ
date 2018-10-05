#!/usr/bin/env python
import pika
import cx_Oracle
import json
import os
import logging

logging.basicConfig(filename='error1.txt', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='hello')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    json_data = json.loads(body.decode("utf-8").replace("'", '"'))
    # print(json_data)
    # print(type(json_data))
    if isinstance(json_data, dict):
        result = insert_data(json_data)
    elif isinstance(json_data, list):
        for data in json_data:
            result = insert_data(data)
    # for data in json_data:
    #     result = insert_data(data)
    logger.info(result)

def insert_data(json_data):
    result = {'Run ID':json_data["Run ID"], 'status':True, 'stdout':None, 'stderr':None}
    #print(result)
    os.environ['ORACLE_HOME'] = '/pqmono/oracle/product/112'
    host = 'scan-capf-11.oracleoutsourcing.com'
    port = '10110'
    db = 'pcapqo'
    user = 'QMON_OD'
    password = 'CV3kc_s7'

    dsn_tns = cx_Oracle.makedsn(host, port, service_name=db)
    try:
        conn = cx_Oracle.connect(user, password, dsn_tns)
        cursor = conn.cursor()
    except cx_Oracle.DatabaseError as exp:
        error,= exp.args
        result['status']=False
        result['stderr']=error.message
        #logger.error(exp)
        return result

    rows = ['"' + x + '"' for x in json_data.keys()]
    rows = ', '.join(rows)
    values = ["'" + x + "'" for x in json_data.values()]
    values_str = ', '.join(values)
    command = "insert into PSOFT_JOB_DETAILS(" + rows + ") values(" + values_str + ")"
    #logger.info(command)
    # savepoint = "SAVEPOINT  SP_" + json_data["Run ID"] + ";"
    # print(savepoint)
    try:
        cursor.execute(command)
        result['stdout']=command

    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        logger.error(error)
        result['status'] = False
        result['stderr']=error.message
    except cx_Oracle.InterfaceError as exc:
        error, = exc.args
        #logger.error(exc)
        result['status'] = False
        result['stderr'] = error.message
    except cx_Oracle.Error as exc:
        error, = exc.args
        result['status'] = False
        result['stderr'] = error.message
    conn.commit()
    cursor.close()
    return result

channel.basic_consume(callback,
                      queue='hello',
                      no_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()