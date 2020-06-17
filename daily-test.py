import json
import csv
import gzip
import os
import logging
from datetime import date, timedelta
import boto3
import time

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

#target file path parameter
def_prefix='tiles/47/P/NT/2020/5/24/0'
def_local='.'
def_bucket='sentinel-s2-l2a'

today = date.today() - timedelta(days=2)
logfile = "daily-{}.log".format(today.strftime("%Y-%m-%d"))
logging.basicConfig(filename=logfile, filemode='w', format='%(asctime)s %(name)s - %(levelname)s - %(message)s', level=logging.INFO, datefmt='%d-%m %H:%M:%S')
logging.info('Logging start for {}'.format(today.strftime("%Y-%m-%d")))
focus_file_date = today.strftime("%Y/%-m")
logging.info(focus_file_date)
# aws parameter
focus_tiles = [ 'tiles/47/P' , 'tiles/47/Q' , 'tiles/47/N', 'tiles/48/P', 'tiles/48/Q', 'tiles/48/N' , 'tiles/36/D']
bucket = 'sentinel-inventory'
manifest_key = 'sentinel-s2-l2a/sentinel-s2-l2a-inventory/{}T00-00Z/manifest.json'.format(today.strftime("%Y-%m-%d"))
logging.info(manifest_key)




# console handler  
console = logging.StreamHandler()  
console.setLevel(logging.INFO)  
logging.getLogger("").addHandler(console)

start = time.time()



def list_keys(bucket, manifest_key):
    manifest = json.load(s3.Object(bucket, manifest_key).get()['Body'])
    for obj in manifest['files']:
        gzip_obj = s3.Object(bucket_name=bucket, key=obj['key'])
        buffer = gzip.open(gzip_obj.get()["Body"], mode='rt')
        reader = csv.reader(buffer)
        for row in reader:
            yield row


def download_dir(prefix, local, bucket, client=s3_client):
    """
    params:
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - bucket: s3 bucket with target contents
    - client: initialized s3 client object
    """
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket':bucket,
        'Prefix':prefix,
        'RequestPayer':'requester'
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        print(bucket, k , dest_pathname)
        #client.download_file(bucket, k, dest_pathname, ExtraArgs={'RequestPayer': 'requester'})

def download_file(bucket, key, local, client=s3_client):
        prefix = os.path.split(os.path.abspath(key))
        dest_pathname = '{}/'.format(prefix[0])
        file_name = prefix[1]
        #fullpath='./{}{}'.format(dest_pathname,file_name)
        #fullpath=dest_pathname+file_name
        print(dest_pathname,file_name)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            print("creating file path")
            os.makedirs(os.path.dirname(dest_pathname))
        #client.download_file(bucket, key, dest_pathname+file_name, ExtraArgs={'RequestPayer': 'requester'})


def format_bytes(size):
    # 2**10 = 1024
    power = 2**10
    n = 0
    power_labels = {0 : '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    to_return = '{:.2f} {}B'.format(size, power_labels[n])
    return to_return



if __name__ == '__main__':

    number = 0
    counting = 0
    downloads_bytes = 0
    total_file_scan = 0
    focus_file_count = 0
    focus_downloads_bytes = 0
    for bucket, key, filesize, *rest in list_keys(bucket, manifest_key):
        if 'tiles' in key:
            #if def_filter in key:
            if any(s in key for s in focus_tiles):
                number += 1
                downloads_bytes += int(filesize)
                #logging.info(key)
                if focus_file_date in key:
                    logging.info(key)
                    focus_file_count += 1 
                    focus_downloads_bytes += int(filesize)

            #print(number, bucket, key,*rest ,end='\r')
            total_file_scan += 1
            counting += 1
            if counting >= 1000000:
                total_bytes=format_bytes(downloads_bytes)
                total_today_bytes=format_bytes(focus_downloads_bytes)
                #logging.info(total_bytes, number, 'files' ,total_file_scan)
                logging.info("{} {:,} files to download , {:,} scanned, today files : {} {:,}".format(total_bytes,number,total_file_scan,total_today_bytes,focus_file_count))
                counting = 0
            
    end = time.time()
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("excute time {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))