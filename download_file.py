import boto3
import os

s3_client = boto3.client('s3')

def_prefix=''
def_local='./B02.jp2'
def_bucket='sentinel-s2-l2a'
def_key='tiles/47/P/NT/2020/5/24/0/R10m/B02.jp2'

def download_file(bucket, key, local, client=s3_client):
        prefix = os.path.split(os.path.abspath(key))
        dest_pathname = '{}/'.format(prefix[0])
        file_name = prefix[1]
        #fullpath='./{}{}'.format(dest_pathname,file_name)
        fullpath=dest_pathname+file_name
        print(dest_pathname,file_name)
        if not os.path.exists(os.path.dirname(dest_pathname)):
                print("creating file path")
                os.makedirs(os.path.dirname(dest_pathname))
        if not os.path.isfile(fullpath):
                print('downloading ', key)
                client.download_file(bucket, key, fullpath, ExtraArgs={'RequestPayer': 'requester'})
        else:
                print("file alreadky exist, skip")


download_file(def_bucket,def_key,def_local)