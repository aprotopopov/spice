import re

def get_keys_values(tables):
    keys = []
    values = []
    
    for cell in tables[0].find_all('tr', class_='left'):
        keys.append(cell.find('span').text)
        values.append(cell.find_next_sibling().find('span').text)

    for cell in tables[1].find_all('tr', class_='left'):
        key_cell = cell.find('span').text
        val_cell = re.sub('\s+', ' ', cell.find_next_sibling().find('span').text).strip()
        values.append(val_cell)
        keys.append(key_cell)

    # fill sub tables
    table_keys_set = set()
    for cells in tables[2].find_all('td', class_='colspan'):
        title_tag = cells.find('span', class_='title')
        
        if not title_tag:
            title_tag = cells.parent.find_previous_sibling().find('span', class_='title')
            title_value = title_tag.parent.parent.find_next_sibling()
            keys.append(title_tag.text)
            values.append(title_value.find('span').text)
            table_keys_set.update([title_tag.text])
            
        title = title_tag.text
        keys_tags = cells.find_all('tr', class_='left')
          
        for key in keys_tags:
            key_name = re.sub('\s+', ' ', key.text).strip()
            table_keys_set.update([key_name])
            keys.append(title + key_name)
            values.append(key.find_next_sibling().find('span').text)

    # fill other cells
    for cell in tables[2].find_all('tr', class_='left'):
        key_cell = cell.find('span').text
        if key_cell in table_keys_set:
            continue
        val_cell = re.sub('\s+', ' ', cell.find_next_sibling().find('span').text).strip()
        values.append(val_cell)
        keys.append(key_cell)
    return keys, values

def get_kadastr_number(soup):
    return soup.find('th', string=re.compile('Кад')).parent.parent.parent.find('td').text

def get_address_anketa(soup):
    return (soup.find('span', string=re.compile('Анкета дома')).find_next_sibling().
        find('span', class_=re.compile('loc_name_ohl')).text)