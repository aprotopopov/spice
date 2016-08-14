import datetime as dt
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from get_data_from_form import get_keys_values, get_kadastr_number, get_address_anketa

url_pref_house = 'https://www.reformagkh.ru'
url_prefix = 'https://www.reformagkh.ru/myhouse'

def get_range_pages(num_house_pages, num_per_page=100):
    if num_house_pages%num_per_page == 0:
        range_pages = num_house_pages//num_per_page
    else:
        range_pages = num_house_pages//num_per_page + 1
    return range_pages

def get_num_pages(link_region):
    return int(link_region.parent.parent.find_next_sibling().find('span').text)


def get_link_houses(browser, link_region, district=False):
    start_time = dt.datetime.now()
    num_house_pages = get_num_pages(link_region)
    range_pages = get_range_pages(num_house_pages)
    if district:
        start_link_region = url_pref_house + link_region.attrs['href']
    else:
        start_link_region = url_prefix + link_region.attrs['href']

    browser.get(start_link_region)
    link_houses = []
    for i in range(range_pages):
        browser.get('https://www.reformagkh.ru/myhouse?sort=name&order=asc'
            '&page={}&limit=100'.format(i+1))
        element = WebDriverWait(browser, 5).until(
            EC.presence_of_element_located((By.XPATH, "//tr[1]/td[1]/a"))
        )
        s = BeautifulSoup(browser.page_source, 'lxml')
        link_on_page = s.find_all('tbody')[1].find_all('a')
        link_houses.extend(url_pref_house + link.attrs['href'] for link in link_on_page)
        print('link houses page {} is parsed with {:.2f} seconds'.format(i + 1, 
            (dt.datetime.now() - start_time).total_seconds()))
    return link_houses



def get_list_key_value_for_area(link_houses, browser, show_interval=10):
    start_time = dt.datetime.now()
    start_time_iteration = dt.datetime.now()
    list_keys = []
    list_values = []
    for i, link_house in enumerate(link_houses):
        # fill keys and values from form
        keys, values = get_keys_values_for_area_single(link_house, browser)

        list_keys.append(keys)
        list_values.append(values)

        if i % show_interval == 0:
            now = dt.datetime.now()
            print('{} house is parsed with total {:.2f} seconds, {:.2f}'
                ' iteration time'.format(i, (now - start_time).total_seconds(),
                    (now - start_time_iteration).total_seconds()))

            start_time_iteration = dt.datetime.now()

    return list_keys, list_values



def get_keys_values_for_area_single(link_house, browser):
    try:
        browser.get(link_house)
        # wait until last element in table will be present
        element = WebDriverWait(browser, 15).until(
            EC.presence_of_element_located((By.XPATH, "//tr[27]/td[1]/span"))
        )
    except Exception as e:
        print(repr(e), link_house)
    s = BeautifulSoup(browser.page_source, 'lxml')
    tables = s.find_all('table', class_='col_list')

    # fill keys and values from form
    keys, values = get_keys_values(tables)

    keys.append('Кадастровый номер')
    values.append(get_kadastr_number(s))

    keys.append('Анкета дома')
    values.append(get_address_anketa(s))
    return keys, values
