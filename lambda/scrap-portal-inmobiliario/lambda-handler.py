import os
import boto3
from selenium import webdriver
import pandas as pd
import portalinmboliario as pi
from datetime import datetime
import json

#Truco especial para guardar/leer con pandas directo en bucket
if os.environ.get('AWS_EXECUTION_ENV') is not None:
    import sys
    import subprocess
    subprocess.call('pip install s3fs -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    sys.path.insert(1, '/tmp/')
    import s3fs

def main(event, context):
    in_lambda = os.environ.get('AWS_EXECUTION_ENV') is not None
    if in_lambda:
        from selenium.webdriver.chrome.options import Options
        options = Options()
        options.binary_location = '/opt/headless-chromium'
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--single-process')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome('/opt/chromedriver',chrome_options=options)
        bucket = os.environ.get('ENV_S3_BUCKET')
        results_prefix = os.environ.get('ENV_S3_PREFIX')
        region_name = os.environ.get('ENV_REGION_NAME')
        sqs_queue_url = os.environ.get('ENV_SQS_QUEUE')

    else:
        from webdriver_manager.chrome import ChromeDriverManager
        driver = webdriver.Chrome(ChromeDriverManager().install())
        bucket ='portal-inmobiliario-scanner-datalake3a44659d-vg3nht2qiz9e'
        results_prefix = '1-results'
        region_name ='us-west-1'
        sqs_queue_url = 'https://sqs.us-west-1.amazonaws.com/844626608976/portal-inmobiliario-scanner-searchesfailed8CDB2A06-1TPJ5JOKZNRFB'

    sqs = boto3.client('sqs', region_name=region_name)
    msgs = sqs.receive_message(QueueUrl=sqs_queue_url, MaxNumberOfMessages=1)

    if not("Messages" in msgs):
        print('No hay mensajes en la cola!')
        return {
            'statusCode': 200,
            'body': event
        }
    
    rec = msgs['Messages'][0]
    print('Procesando {}...'.format(rec))


    driver.implicitly_wait(15)
    driver.get("https://www.portalinmobiliario.com") #Cargamos la página principal
    

    
    receipt_handle = rec['ReceiptHandle']
    body = rec['Body']

    search = json.loads(body)

    print (search)

    operacion = search['operacion']
    categoria = search['categoria']
    busqueda = search['busqueda']
    min_bedrooms = search['min_bedrooms']
    max_bedrooms = search['max_bedrooms']
    max_price_uf = search['max_price_uf']
    min_price_uf = search['min_price_uf']
    min_bathrooms = search['min_bathrooms']
    min_m2 = search['min_m2']


    resultados = pi.resultados(
        driver, operacion=operacion, 
        categoria=categoria, busqueda=busqueda, 
        min_bedrooms=min_bedrooms, max_bedrooms=max_bedrooms, 
        min_price_uf =min_price_uf,
        max_price_uf=max_price_uf, 
        min_bathrooms=min_bathrooms, min_m2=min_m2)

    driver.close()
    driver.quit()

    now = datetime.now()

    resultados['fechahora_rastreo'] = now.strftime("%Y-%m-%d %H:%M:%S")
    resultados['fecha_rastreo'] = now.strftime("%Y-%m-%d")
    resultados['operacion'] = operacion
    resultados['categoria'] = categoria
    resultados['busqueda'] = busqueda

    if not in_lambda:
        resultados.to_csv('resultados_{}.csv'.format(now.strftime("%Y%m%d_%H%M%S")),sep=';',index=False)

    dest_location = 's3://{}/{}/{}/resultados_{}_{}_{}.csv'.format(
        bucket,
        results_prefix,
        now.strftime("%Y/%m/%d"),
        categoria,
        busqueda.replace(' ','_'),
        now.strftime("%Y%m%d_%H%M%S")
    )
    resultados.to_csv(dest_location,sep=';',index=False)

    
    sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handle)
    
    return {
        'statusCode': 200,
        'body': event
    }


if os.environ.get('AWS_EXECUTION_ENV') is None:
    main(0,0)