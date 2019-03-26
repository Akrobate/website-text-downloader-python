# -*- coding: utf-8 -*-

from queue import Queue
from threading import Thread
from threading import Lock
import threading
import time
import requests
import pandas as pd
from urllib.parse import urljoin, urlparse
import os
import sys
from bs4 import BeautifulSoup

# configurations
file_path = './data/'
input_filename = 'websites_url_list.csv'
logs_filename = 'results.csv'
number_of_workers = 50


lock_log_file_write = Lock()
lock_log_data_append = Lock()

# Prepare source file
source_df = pd.read_csv(file_path + input_filename)

if os.path.isfile(file_path + logs_filename):
    output_df = pd.read_csv(file_path + logs_filename)
    print(source_df.shape)
    source_df = source_df.drop(source_df[source_df['id'].isin(output_df['id'])].index)
    print(source_df.shape)
else:
    output_df = pd.DataFrame(columns = ['id', 'status_code', 'size', 'pages_num', 'error'])

def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def url_download(url):
    error = ''
    status_code = 0
    text = ''
    real_url = ''
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    try:
        r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        status_code = r.status_code
        if status_code == 200:
            if 'text' in r.headers['content-type']:
                text = r.text
            else:
                error = 'notTextFile'
            real_url = r.url

    except requests.exceptions.Timeout:
        error = 'Timeout'
    except requests.exceptions.TooManyRedirects:
        error = 'TooManyRedirects'
    except requests.exceptions.RequestException:
        error = 'RequestException'
    except KeyError:
        error = 'KeyErrorContentType'
    except Exception:
        error = 'UnknownContentException'

    return status_code, text, error, real_url


def process_crawl(id, url):
    global output_df

    status_code, text, error, final_url = url_download(url)
    destination_directory = file_path + 'raw_data/' + str(id) + '/'
    leaf_page_num = 0
    if status_code == 200 and error == '':
        
        if not os.path.exists(destination_directory):
            os.makedirs(destination_directory)

        with open(destination_directory + 'home.html','w', encoding="utf-8") as file: 
            file.write(text) 
            file.close()
     
        # create a BeautifulSoup object from the HTML: soup
        soup = BeautifulSoup(text, "lxml")
        
        # Find all 'a' tags (which define hyperlinks): a_tags
        a_tags = soup.find_all('a')
        
        found_link = []

        for link in a_tags:
            link_url = link.get('href')
            formated_link_url = urljoin(final_url, link_url)
            
            if (not link_url is None 
                and urlparse(final_url).hostname in formated_link_url 
                and not link_url.startswith("#")
                and not formated_link_url in found_link):
                
                found_link.append(formated_link_url)
                
                leaf_status_code, leaf_text, leaf_error, leaf_final_url = url_download(formated_link_url)
                if leaf_status_code == 200 and leaf_error == '':
                    leaf_page_num += 1
                    with open(destination_directory + 'page_' + str(leaf_page_num) + '.html','w', encoding="utf-8") as file: 
                        file.write(leaf_text) 
                        file.close()
    
    log = {
            'id': id,
            'status_code': status_code,
            'size': get_size(destination_directory),
            'pages_num': leaf_page_num,
            'error': error
    }
    lock_log_data_append.acquire()
    print(log)
    output_df = output_df.append(log, ignore_index=True)
    lock_log_data_append.release()
    return True

def worker_action(q):
    while True:
        task = q.get()
        if task is None:
            break
        id, url = task
        process_crawl(id, url)
        lock_log_file_write.acquire()
        output_df.to_csv(file_path + logs_filename, index=False)
        lock_log_file_write.release()
        q.task_done()

q = Queue(maxsize = 0)
workers = []

for i in range(number_of_workers):
    worker = Thread(target=worker_action, args=(q,))
    worker.setDaemon(True)
    worker.start()
    workers.append(worker)

source_df.apply(lambda row : q.put( (row['id'], row['website_url']) ),  axis=1)

q.join()

# insert stop signal to workers
for i in range(number_of_workers):
    q.put(None)

# waiting for workers to stop
for t in workers:
    t.join()

output_df.to_csv(file_path + logs_filename, index=False)
