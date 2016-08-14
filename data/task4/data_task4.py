
# coding: utf-8

# ### Данные Москвы и Московской области с сайта "Реформа ЖКХ"

# В данном отчете представлен код для скачивания данных по домам Москвы и Московской области с сайта [Реформа ЖКХ]('https://www.reformagkh.ru'). Затем производится объединение в общий `DataFrame`.Представлено несколько подходов, для последовательного скачивания с помощью `selenium` и `PhantomJS`, а также вариант в многопоточном исполнении с помощью модуля `multithreading`.  
# Из-за недоступности сайта с одного айпи все необходимые данные не удалось загрузить. Были опробованы вариант с proxy из бесплатных, но скорость оставляла желать лучшего. Попытка с tor и socks5 для смены айпи через фиксированное количество запросов (похоже, что около 500) пока не увенчалась успехом, но это вопрос времени. Также было замечано, что с помощью запросов с PhantomJS после 5-ти запросов длительностью каждой 1-2 секунды была пауза в районе 60 секунд. С Firefox подобного поведения не наблюдалось.   
# Далее представлен блок с импортами, а далее с основными операциями.

# In[1]:

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import itertools as it
import datetime as dt
from multiprocessing.pool import ThreadPool as Pool

# user defined modules and functions
from get_data_from_form import get_keys_values, get_kadastr_number, get_address_anketa
from get_links_functions import (get_link_houses, get_num_pages, 
    get_list_key_value_for_area, get_keys_values_for_area_single)

# seleinum imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# Основной код для скачивания данныех без использования proxy, смены айпи:

# In[ ]:

# browser = webdriver.PhantomJS()
browser = webdriver.Firefox()

url_pref_house = 'https://www.reformagkh.ru'
url_prefix = 'https://www.reformagkh.ru/myhouse'
url_mos_area = 'https://www.reformagkh.ru/myhouse?tid=2281126'
url_mos = 'https://www.reformagkh.ru/myhouse?tid=2280999'

urls = [url_mos_area, url_mos]

multithread = False

if multithread:
    num_workers = 4
    pool = Pool(num_workers)
#     browsers = [webdriver.PhantomJS() for i in range(num_workers)]
    browsers = [webdriver.Firefox() for i in range(num_workers)]

for url in urls:
    # r = requests.get(url)
    # s = BeautifulSoup(r.text, 'lxml')
    browser.get(url)
    s = BeautifulSoup(browser.page_source, 'lxml')
    list_full_keys = []
    list_full_values = []
    list_full_names = []
    list_full_urls = []
    link_areas = s.find('table', class_='col_list tree').find_all('a')
    for link_area in link_areas[:1]:
        start_link_area = url_prefix + link_area.attrs['href']
        print(link_area.text)
        # r1 = requests.get(start_link_area)
        # s1 = BeautifulSoup(r1.text, 'lxml')
        requests.get(start_link_area)
        s1 = BeautifulSoup(browser.page_source, 'lxml')
        list_link_houses = []

        district = s1.find('table', class_='col_list tree') is not None
        if district:
            table_regions = s1.find_all('table', class_='col_list tree')
            link_regions = []
            for table_region in table_regions:
                link_regions.extend(table_region.find_all('a'))                

        else:
            # make list ling_regions with single link_area
            link_regions = [link_area]

        for link_region in link_regions:
            link_houses = get_link_houses(browser, link_region, district=district)
            link_houses = [link.split('?')[0] for link in link_houses]
            list_full_urls.extend(link_houses)
            if multithread:
                repeat_times = len(link_houses)//num_workers + 1
                browsers_list = list(it.chain(*it.repeat(browsers, times=repeat_times)))[:len(link_houses)]
                list_keys, list_values = pool.starmap(
                    get_keys_values_for_area_single, list(zip(link_houses, browsers_list)))
            else:
                list_keys, list_values = get_list_key_value_for_area(link_houses, browser, test=True)

            list_full_keys.extend(list_keys)
            list_full_values.extend(list_values)

            num_pages = get_num_pages(link_region)
            list_full_names.extend(list(it.repeat(link_region.text, times=num_pages)))
            
        # create DataFrame after each link_area and save it
        df = pd.DataFrame(list_full_values, columns=list_full_keys[0])
        name_series = pd.Series(list_full_names, name='Название региона')
        url_series = pd.Series(list_full_urls, name='Ссылки')
        if district:
            district_series = pd.Series(list(it.repeat(link_area.text, times=len(list_full_values))), 
                                    name="Название района")
        else:
            district_series = pd.Series(list(it.repeat(np.nan, times=len(list_full_values))), 
                                    name="Название района")   
        # join DataFrame and Series to final df and save for each region
        df = pd.concat([name_series, df, url_series, district_series], axis=1)
        df.to_csv('data/{}.csv'.format(link_area.text), sep=';', index=False)


# Функция для добавления proxy, не каждый proxy сервер стабильно работает + скорость мала.

# In[2]:

def load_proxy(PROXY_HOST,PROXY_PORT):
        fp = webdriver.FirefoxProfile()
        fp.set_preference("network.proxy.type", 1)
        fp.set_preference("network.proxy.http",PROXY_HOST)
        fp.set_preference("network.proxy.http_port",int(PROXY_PORT))
        fp.set_preference("general.useragent.override","whater_useragent")
        fp.update_preferences()
        return webdriver.Firefox(firefox_profile=fp)


# Вариант с использованием [Tor](https://www.torproject.org/docs/debian.html.en) и [Privoxy ](http://www.privoxy.org/user-manual/installation.html). Для данного варианта необходимо реализовать еще принудительную смены личности в Tor, и тогда можно запускать web-scraping.

# In[3]:

from selenium.webdriver.common.proxy import *

port = "8118" #The Privoxy (HTTP) port
myProxy = "127.0.0.1:"+port

proxy = Proxy({
    'proxyType': ProxyType.MANUAL,
    'httpProxy': myProxy,
    'ftpProxy': myProxy,
    'sslProxy': myProxy,
    'noProxy': '' # set this value as desired
    })
driver = webdriver.Firefox(proxy=proxy)


# Для смены личности можно использовать [stem](https://stem.torproject.org/index.html), для этого необходимо выполнить следующий код, с предварительной настройкой согласно [документации](https://stem.torproject.org/tutorials/the_little_relay_that_could.html):

# In[ ]:

from stem import Signal
from stem.control import Controller

with Controller.from_port(port = 9051) as controller:
  controller.authenticate()
  controller.signal(Signal.NEWNYM)


# Далее представлен `DataFrame` для района Власиха (ЗАТО) Московской области, которые удалось извлечь с сайта с помощью функций из модуля `get_data_from_form`.

# In[5]:

df = pd.read_csv('data/Власиха (ЗАТО).csv', sep=';')


# In[6]:

df.head()


# ### Выводы

# На данный момент реализованы функции с обработкой формы конкретного дома и проверена на нескольких разных формах. Также реализованы функции с рекурсивным обходом домов Москвы и Московской области, но полностью не протестированы и данные не получены ввиду блокировки сайтом "Реформа ЖКХ". Вариант с реализацией proxy, tor пока реализовать не удалось.
