from bs4 import BeautifulSoup
import requests
import pandas as pd
import threading
import re
from urllib.parse import urljoin


def get_page(url):
    '''
    Function wich make get request on given url and returns response text
    :param url:
    :return:
    '''
    headers = {
        'authority': url,
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'ru-UA,ru;q=0.9,uk-UA;q=0.8,uk;q=0.7,ru-RU;q=0.6,en-US;q=0.5,en;q=0.4',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    }
    try:
        print(f"Making request to {url}")
        response = requests.get(f"https://{url}", headers=headers, verify=False)
        if not response.status_code == 200:
            return None
        return response.text
    except Exception as e:
        print(f"Exception occured in (get_page): {e}")


def find_contact_link(soup, base_url):
    links = soup.find_all('a')

    for link in links:
        link_text = link.get_text()
        link_href = link.get('href')
        try:
            if link_text and 'contact' in link_text.lower():
                contact_link = link_href
            elif link_href and 'contact' in link_href.lower():
                contact_link = link_href
            else:
                continue

            if not contact_link.startswith(('http://', 'https://')):
                contact_link = urljoin(base_url, contact_link)
            else:
                contact_link = contact_link.split('://', 1)[-1]
                return contact_link
        except Exception as e:
            print("Something went wrong: ", e)
    return None


def parse_page(data, url):
    '''
    Because of uniqueness of every page parse page using regular expressions then return dictionary with results
    :param data:
    :param url:
    :return:
    '''
    soup = BeautifulSoup(data, 'lxml')
    contacts = {}

    contact_us_page = find_contact_link(soup, url)
    if contact_us_page:
        new_data = get_page(contact_us_page)
        if new_data:
            soup = BeautifulSoup(new_data, "lxml")

    phone_pattern = re.compile(r'(\+?\d{1,3}\s?-?\(?\d{2,3}\)?\s?-?\d{2,3}\s?-?\d{2,3}\s?-?\d{2,3})')
    phone_matches = phone_pattern.findall(data)
    if phone_matches:
        contacts['phone'] = " ".join(phone_matches[:3])

    email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    email_matches = email_pattern.findall(data)
    if email_matches:
        contacts['email'] = email_matches[0]
    skype_pattern = re.compile(r'skype:[\w\.-]+')
    skype_matches = skype_pattern.findall(data)
    if skype_matches:
        contacts['skype'] = skype_matches[0].split(':')[-1]

    address_tag = soup.find('address')
    if address_tag:
        contacts['address'] = address_tag.get_text()

    social_links = soup.find_all('a', class_='social-link')
    for link in social_links:
        if 'facebook.com' in link['href']:
            contacts['facebook'] = link['href']
        elif 'twitter.com' in link['href']:
            contacts['twitter'] = link['href']
        elif 'instagram.com' in link['href']:
            contacts['instagram'] = link['href']
    return contacts


def get_urls(table_name):
    '''
    Parse all urls without duplicates from excel file
    :return:
    '''
    urls_result = set()
    df = pd.read_excel("USA Services.xlsx")
    urls_col = df["website"]
    for row in urls_col:
        if pd.isna(row):
            continue
        urls = row.split(", ")
        for url in urls:
            urls_result.add(url)
    return urls_result


def process_page(url, df: pd.DataFrame, location, keyword):
    '''
    Receive page text then parsing it and append into result file
    :param url:
    :param df:
    :param location:
    :param keyword:
    :return:
    '''
    data = get_page(url)
    # new_row = pd.DataFrame(index=[0])
    if not data:
        return df
    else:
        data = parse_page(data, url)
    #     for key, value in data.items():
    #         new_row.loc[0, key] = value
    # new_row.loc[0, 'website'] = url
    # print(new_row)
    data["website"] = url
    data["location"] = location
    data["keyword"] = keyword
    df.loc[len(df.index)] = data
    print(data)
    return df


def main():
    '''
    Entry point of program
    Receive all urls from the excel file and start process each url using threads for time saving
    :return:
    '''
    urls = list(get_urls(""))
    # process_per_iter = 10
    sites_range = len(urls)
    existing_table = pd.read_excel("USA Services.xlsx")
    try:
        for i in range(sites_range):
            existing_table = process_page(urls[i], existing_table, "Florida", "Chiropractor")
    except Exception as e:
        print(f"Something went wrong in (main): {e}")
    finally:
        existing_table.to_excel("USA Services.xlsx", index=False)
        print(existing_table)


if __name__ == "__main__":
    main()


