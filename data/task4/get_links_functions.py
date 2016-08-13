from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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
    num_house_pages = get_num_pages(link_region)
    range_pages = get_range_pages(num_house_pages)
    if district:
        start_link_region = url_pref_house + link_region.attrs['href']
    else:
        start_link_region = url_prefix + link_region.attrs['href']

    browser.get(start_link_region)
    link_houses = []
    for i in range(range_pages)[:]:
        browser.get('https://www.reformagkh.ru/myhouse?sort=name&order=asc&page={}&limit=100'.format(i+1))
        element = WebDriverWait(browser, 5).until(
            EC.presence_of_element_located((By.XPATH, "//tr[1]/td[1]/a"))
        )
        s = BeautifulSoup(browser.page_source, 'lxml')
        link_on_page = s.find_all('tbody')[1].find_all('a')
        link_houses.extend(url_pref_house + link.attrs['href'] for link in link_on_page)
        print('link houses page {} is done'.format(i))
    return link_houses



def get_list_key_value_for_area(link_houses):
    list_keys = []
    list_values = []
    for i, link_house in enumerate(link_houses):
        # fill keys and values from form
        keys, values = get_keys_values_for_area_single(link_house)

        list_keys.append(keys)
        list_values.append(values)
        print(i, 'house is done')
    return list_keys, list_values



def get_keys_values_for_area_single(link_house):
    try:
        browser = webdriver.PhantomJS()
        browser.get(link_house)
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
