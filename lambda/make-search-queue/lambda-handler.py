import boto3
import os
import pandas as pd
import s3bucket
import uuid
import json

def log_banner(text):
    banner_width = len(text)+8
    print (banner_width*'*')
    print ('*** {} ***'.format(text))
    print (banner_width*'*')

def main(event, context):
    if os.environ.get('AWS_EXECUTION_ENV') is None:
        sqs_queue_url = 'https://sqs.us-west-1.amazonaws.com/844626608976/portal-inmobiliario-scanner-searches1E9308C2-D0F6X35EC82Y'
        bucket ='portal-inmobiliario-scanner-datalake3a44659d-vg3nht2qiz9e'
        prefix = '0-search'
        region_name ='us-west-1'
    else:
        sqs_queue_url = os.environ.get('ENV_SQS_QUEUE')
        bucket = os.environ.get('ENV_S3_BUCKET')
        prefix = os.environ.get('ENV_S3_PREFIX')
        region_name = os.environ.get('ENV_REGION_NAME')

    
    sqs = boto3.client('sqs', region_name=region_name)

    log_banner('Log de Ejecución')

    files = s3bucket.list_files(bucket, prefix)
    print('Se han encontrado {} archivo(s) en el {}/{}'.format(len(files), bucket, prefix))
    sqs_messages = []
    for file in files:
        if file['Size'] >0:
            s3url = 's3://{}/{}'.format(bucket, file['Key'])
            searches = pd.read_csv(s3url , sep=';')
            for elem in searches.T.to_dict().values():
                sqs_messages.append({
                    'Id': str(uuid.uuid4()),
                    'MessageBody': json.dumps(elem)
                })
                
    chunks = [sqs_messages[i:i + 10] for i in range(0, len(sqs_messages), 10)]

    for chunk in chunks:
        print ('** enviando', len(chunk), 'mensajes a la cola', sqs_queue_url)
        response = sqs.send_message_batch(
            QueueUrl=sqs_queue_url,
            Entries=chunk)

        #print (response)
        if 'Successful' in response:
            print ('Respuesta Exitosa:', len(response['Successful']))
        if 'Failed' in response:
            print ('Respuesta Fallida:', response['Failed'])



    print('\n---Fin de Ejecución---\n')

    return {
        'statusCode': 200,
        'body': "OK"
    }


if os.environ.get('AWS_EXECUTION_ENV') is None:
    main(0,0)