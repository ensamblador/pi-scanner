# %%

import boto3
s3client = boto3.client('s3')


def descarga_archivo(bucket, key, base_path, filename):
    with open(base_path+filename, "wb") as data:
        s3client.download_fileobj(bucket, key, data)
    print('Archivo {}/{} descargado en {}/{}'.format(bucket, key, base_path, filename))

    return True


def sube_archivo(path, filename, bucket, key):
    with open(path+filename, 'rb') as data:
        s3client.upload_fileobj(data, bucket, key)
    return True


def borra_archivo(bucket, key):
    s3client.delete_object(Bucket=bucket, Key=key)
    return True

def list_files(bucket, prefix):

    paginator = s3client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix
    )
    total_contents = []

    for elem in response_iterator:
        if 'Contents' in elem:
            total_contents = total_contents + elem['Contents']

    return total_contents
