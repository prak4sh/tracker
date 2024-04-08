import httpx
import random
from bs4 import BeautifulSoup
import re
from rich import print
from discord_webhook import DiscordWebhook, DiscordEmbed
from datetime import datetime, date, timedelta
import time
import calendar
from urllib.parse import urlparse, urljoin
import pandas as pd
import sys

from dotenv import load_dotenv
import os

from seatable_api.constants import ColumnTypes
from seatable_api import Base, context

load_dotenv()
SEATABLE_API = os.getenv('SEATABLE_API')
SEATABLE_URL = 'https://cloud.seatable.io'
BASE = Base(SEATABLE_API, SEATABLE_URL)
BASE.auth()

DELIVERY_WEBHOOK = os.getenv('DELIVERY_WEBHOOK')
STOCK_WEBHOOK = os.getenv('STOCK_WEBHOOK')
PRICE_WEBHOOK = os.getenv('PRICE_WEBHOOK')
SEND_NOTIFICATION = True

PRODUCT_TABLE = 'Products'
PRODUCTS_COLUMNS = [
    ('ASIN',ColumnTypes.TEXT),
    ('CHANGE_PERCENT', ColumnTypes.NUMBER),
    ('URL', ColumnTypes.TEXT),
    ('TITLE', ColumnTypes.TEXT),
    ('CURRENCY', ColumnTypes.TEXT),
    # ('AVAILABILITY', ColumnTypes.TEXT)
    ('PRICE', ColumnTypes.NUMBER),
    ('DELIVERY_DATE', ColumnTypes.DATE),
    ('Updated Time', ColumnTypes.MTIME)
]
DOMAINS = ['NL','DE']

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Accept-Language': 'en-US,en;q=0.9',
    'Content-Type': 'application/json'
}
USER_AGENTS = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2224.3 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.137 Safari/4E423F',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:70.0) Gecko/20190101 Firefox/70.0',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:64.0) Gecko/20100101 Firefox/64.0',
        ]

SEATABLE_DF = pd.DataFrame()
NOTIFY_DAY = 2

not1, not2, not3, not4, not5 = False, False, False, False, False

START = True

def _requests(url):
    global START
    HEADERS['Referer'] = url
    if 'amazon.de' in url:
        HEADERS['Authority'] = 'www.amazon.de'
    elif 'amazon.nl' in url:
        HEADERS['Authority'] = 'www.amazon.nl'
    elif 'amazon.it' in url:
        HEADERS['Authority'] = 'www.amazon.it'
    while True:
        try:
            HEADERS['User-Agent'] = get_UA()
            response = httpx.get(url, headers=HEADERS)
            if response.status_code == 200:
                print(f'[green]{time_now()},[/green] Url: {url}, Status: {response.status_code}')
                break
            elif response.status_code == 404:
                print(f'[green]{time_now()},[/green] Url: {url}, Status: {response.status_code}')
                return None 
            else:
                print(f'[green]{time_now()},[/green] Url: {url}, Status: {response.status_code}')
                randtime = random.randint(5,10)
                time.sleep(randtime)
                if not START and 'add.html' not in url:
                    break
        except Exception as e:
            print(f'[red]{time_now()},[/red] Error [REQUEST]: {e}')
            time.sleep(5)
    return response

def _soup(response):
    return BeautifulSoup(response.text, 'html.parser')

def time_now():
    return datetime.now().strftime('%Y-%m-%d %H:%M')

def print_info(msg, status=1):
    if status == 1:
        print(f'[green]{time_now()},[/green] {msg}')
    elif status == 0:
        print(f'[red]{time_now()}, {msg} [/red]')
    elif status == 2:
        print(f'[red]{time_now()}, {msg} [/red]', end="\r")

def check_table_existence(table_name):
    metadata = BASE.get_metadata()
    for table in metadata['tables']:
        if table['name'] == table_name:
            return True
    return False

def create_new_table(table_name):
    table = BASE.add_table(table_name, lang='en')
    if table:
        print_info(f'Creating a new Table: {table_name}')
    else:
        print_info('Failed to create the table.')

def check_columns(table_name, columns):
    existing_columns = [x['name'] for x in BASE.list_columns(table_name)]
    existing_columns = list(set(existing_columns))
    for col_i, column in enumerate(columns):
        column_name = column[0]
        if col_i == 0 and 'Name' in existing_columns:
            print_info(f'Creating a new column: {column_name}     ')
            BASE.rename_column(table_name=table_name, column_key='Name', new_column_name=column_name)
        elif column[0] not in existing_columns:
            print_info(f'Creating a new column: {column_name}      ')
            BASE.insert_column(table_name=table_name, column_name=column[0], column_type=column[1], column_data=None)

def seatable_dataframe(table_name, columns):
    if not check_table_existence(table_name):
        create_new_table(table_name)
    check_columns(table_name, columns)
    start = 0
    limit = 1000
    all_rows = []
    while True:
        rows = BASE.list_rows(table_name, start=start ,limit=limit)
        if len(rows) > 0:
            all_rows += rows
            start += limit
        else:
            break
        print_info(f'Total rows collected from {table_name} database: {len(all_rows)}',2)
    print_info(f'Total rows collected from {table_name} database: {len(all_rows)}', 1)
    df = pd.DataFrame(all_rows)
    return df

def get_asins(df):
    if 'ASIN' in df.columns:
        asins = df['ASIN'].dropna().to_list()
    else:
        asins = []
    return asins

def asin_from_regex(url):
    pattern = r'/dp/([A-Z0-9]{10})'
    match = re.search(pattern, url)
    if match:
        asin = match.group(1)
        return asin
    else:
        return None

def float_price(price_text):
    pattern0 = r"\d+\,\d+"
    pattern1 = r"\d+\.\d+"
    pattern2 = r"\d+"
    currency_pattern = r"[^\d,.]+"
    found0 = re.search(pattern0, price_text)
    found1 = re.search(pattern1, price_text)
    found2 = re.search(pattern2, price_text)
    currency_found = re.search(currency_pattern, price_text)
    if found0:
        price = found0.group(0).replace(',','.')
    elif found1:
        price = found1.group(0)
    elif found2:
        price = found2.group(0)
    else:
        price = 0
    try:
        price = int(price)
    except:
        price = float(price)
    if currency_found:
        currency = currency_found.group(0).strip()
    else:
        currency = ''
    return price, currency

def get_delivery_dates(delivery_text):
    delivery_text = delivery_text.lower()
    month = None
    months = list(calendar.month_name)[1:]
    for mon_index, mon in enumerate(months):
        mon_index += 1
        if mon.lower() in delivery_text:
            month = mon_index
            break
    day_pattern = r'(\d+)-(\d+)'
    day_match = re.search(day_pattern, delivery_text)
    if day_match:
        start_day = int(day_match.group(1))
        end_day = int(day_match.group(2))
    else:
        # If no range is found, try to find a single day
        day_pattern = r'\d+'
        day_found = re.findall(day_pattern, delivery_text)
        if day_found:
            start_day = end_day = int(day_found[0])
        else:
            return None
    if month:
        first_date = date(2024, month, start_day)
        last_date = date(2024, month, end_day)
        return first_date, last_date
    else:
        return None

def _details(asin, domain):
    title = None
    price = 0
    delivery_date = None
    currency = None
    if domain == 'NL':
        domain_url = 'https://www.amazon.nl'
        url = f'https://www.amazon.nl/dp/{asin}?&language=en_GB'
    elif domain == 'DE':
        domain_url = 'https://www.amazon.de'
        url = f'https://www.amazon.de/dp/{asin}?&language=en_GB'
    elif domain == 'IT':
        domain_url = 'https://www.amazon.it/'
        url = f'https://www.amazon.it/dp/{asin}?&language=en_GB'
    response = _requests(url)
    if response:
        soup = _soup(response)
        asin = soup.find(id="ASIN")
        if asin:
            asin = asin.get('value')
        else:
            asin = asin_from_regex(url)
        title = soup.find(id="productTitle")
        if title:
            title = title.get_text().strip()
        imageUrl = soup.select_one('#imgTagWrapperId img')
        if imageUrl:
            imageUrl = imageUrl.get('src')
        price_div = soup.find(id='corePrice_feature_div')
        if price_div:
            price_text = price_div.find(class_='a-offscreen')
            if price_text:
                price_text = price_text.get_text().strip()
                price, currency = float_price(price_text)
                delivery_block = soup.find(id="deliveryBlockMessage")
                if delivery_block:
                    delivery_text = delivery_block.find(class_='a-text-bold')
                    if delivery_text:
                        delivery_text = delivery_text.get_text()
                        delivery_4m_function = get_delivery_dates(delivery_text)
                        if delivery_4m_function:
                            fist_date, last_date = delivery_4m_function
                            delivery_date = last_date
        if price:
            availability = 'YES'
        else:
            availability = 'NO'
        info = dict()
        info['ASIN'] = asin
        info['DOMAIN'] = domain_url
        info['URL'] = url
        info['TITLE'] = title
        info['PRICE'] = price
        info['AVAILABILITY'] = availability
        info['CURRENCY'] = currency
        info['DELIVERY_DATE'] = delivery_date
        info['IMAGE'] = imageUrl
    else:
        info = {
            'ASIN': asin,
            'Domain': domain,
            'URL': None,
            'TITLE': None,
            'Price': None,
            'AVAILABILITY': None,
            'CURRENCY': None,
            'DELIVERY_DATE': None,
            'IMAGE': None
        }
    print(info)
    return info

def details_from_cart(asin,domain):
    if domain == 'NL':
        domain_url = 'https://www.amazon.nl'
    elif domain == 'DE':
        domain_url = 'https://www.amazon.de'
    elif domain == 'IT':
        domain_url = 'https://www.amazon.it'
    url = f'{domain_url}/dp/{asin}'
    title = None
    price = 0
    currency = None
    delivery_date = None
    product_url = urljoin(domain_url,f'/gp/aws/cart/add.html?ASIN.1={asin}')
    response = _requests(product_url)
    if response:
        soup = _soup(response)
        title = soup.find(class_='sc-product-title')
        if title:
            title = title.get_text().strip()
        price = soup.find(class_='sc-product-price')
        if price:
            price_text = price.get_text().strip()
            price, currency = float_price(price_text)
        else:
            price = 0
        if price:
            availability = 'YES'
        else:
            availability = 'NO'
        imageUrl = soup.select_one('.sc-product-link img')
        if imageUrl:
            imageUrl = imageUrl.get('src')
        info = dict()
        info['ASIN'] = asin
        info['DOMAIN'] = domain
        info['URL'] = url
        info['TITLE'] = title
        info['PRICE'] = price
        info['AVAILABILITY'] = availability
        info['CURRENCY'] = currency
        info['DELIVERY_DATE'] = delivery_date
        info['IMAGE'] = imageUrl
    else:
        info = {
            'ASIN': asin,
            'Domain': None,
            'URL': None,
            'TITLE': None,
            'Price': None,
            'AVAILABILITY': None,
            'CURRENCY': None,
            'DELIVERY_DATE': None,
            'IMAGE': None
        }
    return info

def counttime(next_date):
    today = date.today()
    days_to_add = 1
    days_count = 1
    if today == next_date:
        return 0
    while True: 
        tomorrow = today + timedelta(days=days_to_add)
        if tomorrow != next_date:
            # if tomorrow.weekday() < 5: 
            #     days_to_add += 1
            #     days_count += 1
            # else:
            #     days_to_add += 1
            days_to_add += 1
            days_count += 1
        else:
            break
    return days_count

def read_data_from_database(df, asin):
    if not df.empty:
        return df[df['ASIN'] == asin]
    else:
        return pd.DataFrame()

def send_notification(msg, info ,status):
    global START
    title = info['TITLE']
    url = info['URL']
    if info['AVAILABILITY'] == 'YES':
        availability = ':white_check_mark:'
    else:
        availability = ':x:'
    price = info['PRICE']
    currency = info['CURRENCY']
    if 'nl' in info['DOMAIN'].lower():
        country = ':flag_nl:'
    elif 'de' in info['DOMAIN'].lower():
        country = ':flag_de:'
    else:
        country = ':qesution:'
    asin = info.get('ASIN', ':question:')
    image = info.get('IMAGE', None)
    if price:
        if currency:
            price = f'{currency}{price}'
        else:
            pass
    else:
        price = ':question:'
    delivery = info.get('DELIVERY_DATE', ':question:')
    if START:
        print_info('This is first time')
    if status == 'stock':
        webhook = STOCK_WEBHOOK
    elif status == 'price':
        webhook = PRICE_WEBHOOK
    elif status == 'delivery':
        webhook = DELIVERY_WEBHOOK
    else:
        return None
    if SEND_NOTIFICATION and not START:
        webhook = DiscordWebhook(url=webhook, rate_limit_retry=True)
        embed = DiscordEmbed(title=title, url=url ,color='03b2f8')
        embed.set_thumbnail(url=image)
        embed.set_author(name="prakash", url="")
        embed.set_timestamp()
        embed.add_embed_field(name='Status', value=msg, inline=False)
        embed.add_embed_field(name='ASIN', value=asin, inline=True)
        embed.add_embed_field(name="Price", value=price, inline=True)
        embed.add_embed_field(name="Country", value=country, inline=True)
        embed.add_embed_field(name="Availability", value=availability, inline=True)
        embed.add_embed_field(name="Delivery", value=delivery, inline=True)
        webhook.add_embed(embed)

        response = webhook.execute()
        if response.status_code == 200:
            print(f'[green]{time_now()},[/green] Notification has been sent to discord.')
        else:
            print(f'[red]{time_now()},[/red] Error on sending notification to discord')

def update_data_to_database(infoList, df, table_name, ref):
    if len(infoList) > 0:   
        for column in PRODUCTS_COLUMNS:
            if column[0] not in df:
                df[column[0]] = None
        df2 = pd.DataFrame(infoList)
        changes_df = df.merge(df2, on=ref, suffixes=('_existing', '_website'), how='inner')
        changed_columns = [x for x in changes_df.columns if '_website' in x]
        new_columns = [x.replace('_website','') for x in changed_columns]
        update_rows_data = list()
        for change_data in changes_df.to_dict('records'):
            row_id = change_data['_id']
            changes_row = {
                'row_id': row_id,
                'row': {new_columns[i]:change_data[changed_columns[i]] for i in range(len(changed_columns)) }
            }
            update_rows_data.append(changes_row)
        
        update_rows_parts = [update_rows_data[i:i+1000] for i in range(0, len(update_rows_data), 1000)]
        update_count = 0
        for update_rows_part in update_rows_parts:
            update_count += len(update_rows_part)
            if len(update_rows_part) > 0:
                BASE.batch_update_rows(table_name, rows_data=update_rows_part)
                print_info(f'Number of rows updated: {update_count}', 2)
            else:
                print_info('No new rows updated.')
            print_info(f'Number of rows updated: {update_count}', 1)

def write_data_to_database(infoList, table_name):
    if len(infoList) > 0:
        for index,info in enumerate(infoList):
            BASE.append_row(table_name, info)
            print_info(f'Number of rows added: {index}', 2)

def info_to_database(all_data):
    update_list = []
    new_list = []
    table_name = PRODUCT_TABLE
    global SEATABLE_DF
    for info in all_data:
        asin = info['ASIN']
        asin_db = read_data_from_database(SEATABLE_DF, asin)
        if not asin_db.empty:
            update_list.append(info)
        else:
            new_list.append(info)
    update_data_to_database(update_list, SEATABLE_DF, table_name, ref='ASIN')
    write_data_to_database(new_list, table_name)

def main():
    global notified_delivery
    global notified_price
    global notified_stock
    global START
    global SEATABLE_DF
    SEATABLE_DF = seatable_dataframe(PRODUCT_TABLE, PRODUCTS_COLUMNS)
    all_asins = get_asins(SEATABLE_DF)
    all_data = []
    global not1, not2, not3, not4, not5
    for asin_index, asin in enumerate(all_asins):
        info = dict()
        notified = False
        if asin is None or asin == '':
            continue
        days_count = 0
        if 'NL' in DOMAINS or 'DE' in DOMAINS:
            domain = 'NL'
            info_nl = _details(asin, domain)
            title_nl = info_nl['TITLE']
            price_nl = info_nl.get('PRICE', None)
            if not price_nl:
                info_nl = details_from_cart(asin, domain)

            domain = 'DE'
            info_de = _details(asin, domain)
            title_de = info_de['TITLE']
            price_de = info_de.get('PRICE', None)
            if not price_de:
                info_de = details_from_cart(asin, domain)
            if info_nl['PRICE'] and info_nl['PRICE'] <= info_de['PRICE']:
                info = info_nl
                url = f'https://www.amazon.nl/dp/{asin}?&language=en_GB'
            else:
                info = info_de
                url = f'https://www.amazon.nl/dp/{asin}?&language=en_GB'

        asin = info['ASIN']
        domain = info['DOMAIN']
        currency = info['CURRENCY']
        if isinstance(info['PRICE'], int):
            price = int(info['PRICE'])
        elif isinstance(info['PRICE'], float):
            price = float(info['PRICE'])
        else:
            price = info['PRICE']
        delivery_date = info['DELIVERY_DATE']
        if delivery_date:
            info['DELIVERY_DATE'] = delivery_date.strftime('%Y-%m-%d')
        if delivery_date:
            try:
                days_count = counttime(delivery_date)
                print(f'[green]{time_now()},[/green] #{asin_index + 1}|{len(all_asins)} ASIN: {asin}, domain: {domain}, price: {currency}{price}, Delivery: {delivery_date} (in {days_count} days)')
            except Exception as e:
                print_info(f'Error [main]: {e}')
        else:
            print(f'[green]{time_now()},[/green] #{asin_index + 1}|{len(all_asins)} ASIN: {asin}, domain: {domain}, price: {currency}{price} ')

        database_df = read_data_from_database(SEATABLE_DF, asin)
        database_list = database_df.to_dict('records')
        if len(database_list) > 0:
            title_db = database_list[0].get('TITLE', None)
            url_db = database_list[0].get('URL', None)
            price_db = database_list[0].get('PRICE', None)
            changes_per = database_list[0].get('CHANGE_PERCENT', 0)
            delivery_db = database_list[0].get('DELIVERY_DATE', None)
            if title_db is None or pd.isna(title_db):
                title_db = None
            if not price_db:
                price_db = 0
        else:
            url_db = None

        if url_db:
            if info['TITLE'] is None:
                info['TITLE'] = title_db
            if info['DELIVERY_DATE'] is None:
                info['DELIVERY_DATE'] = delivery_db
            if days_count > NOTIFY_DAY:
                msg = f"ASIN: {asin}, Domain: {domain}, Delivery: {days_count} days"
                notify_msg = f"Delivery is within {days_count} days."
                if not not1:
                    print_info(msg)
                    send_notification(notify_msg, info, 'delivery')
                    not1, not2, not3, not4, not5 = True, False, False, False, False
            if price_db > 0 and price == 0:
                notify_msg = f"Product is Out of Stock."
                print(f'[green]{time_now()},[/green] {notify_msg}')
                if not not2:
                    send_notification(notify_msg, info,'stock')
                    not1, not2, not3, not4, not5 = False, True, False, False, False
            if price_db != price and price > 0:
                if price < price_db:
                    discount = price_db * (changes_per / 100)
                    price_with_discount = price_db - discount
                    discount_per = ((price_db - price) / price_db) * 100
                    drop_msg = f"Price has dopped from {currency}{price_db} to {currency}{price} by {discount_per:.2f}%"
                    print(f'[green]{time_now()},[/green] {drop_msg}')
                    if discount_per > changes_per and not not3:
                        send_notification(drop_msg, info,'price')
                        not1, not2, not3, not4, not5 = False, False, True, False, False
                elif price_db == 0:
                    msg = f"Product is Available now."
                    print(f'[green]{time_now()},[/green] {msg}')
                    if not not4:
                        send_notification(msg, info ,'stock')
                        not1, not2, not3, not4, not5 = False, False, False, True, False
                    
                elif price > price_db:
                    increase = price_db * (changes_per / 100)
                    price_with_increase = price_db + increase
                    increase_per = ((price - price_db) / price_db) * 100
                    rise_msg = f"Price has rised from {currency}{price_db} to {currency}{price} by {increase_per:.2f}%"
                    print(f'[green]{time_now()},[/green] {rise_msg}') 
                    if increase_per > changes_per and not not5:
                        send_notification(rise_msg, info, 'price')
                        not1, not2, not3, not4, not5 = False, False, False, False, True   
        all_data.append(info)
        if len(all_data) >= 50:
            info_to_database(all_data)
            all_data = []
        time.sleep(random.randint(2,5))
    info_to_database(all_data) 
    START = False
        
    pass

def get_UA():
    result = random.choice(USER_AGENTS)
    if os.path.exists('userAgent.txt'):
        with open('userAgent.txt', 'r') as f:
            ua = f.readlines()
            ua = [x.strip() for x in ua]
            result = random.choice(ua)
    return result

def time_spend(start_time):
    now = datetime.now()
    delta = now - start_time
    delta_sec = delta.total_seconds()
    delta_min = delta_sec / 60
    delta_hrs = delta_min / 60
    if delta_sec < 60:
        return "{:.2f} sec".format(delta_sec)
    elif delta_min > 1 and delta_min < 60:
        return "{:.2f} min".format(delta_min)
    elif delta_hrs > 1 and delta_hrs < 60:
        return "{:.2f} hrs".format(delta_hrs) 

if __name__=="__main__":
    while True:
        try:
            start_time = datetime.now()
            main()
            print_info(f'Script completed successfully in {time_spend(start_time)}')
            print('='*50)
            time.sleep(6)
        except Exception as e:
            error_msg = f'Error: {e}'
            print(f'[red]{time_now()},[/red] {error_msg}')
            time.sleep(60)
    