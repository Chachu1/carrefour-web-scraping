from urllib.parse import urljoin, urlencode
import requests
import json

global_headers = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en-GB;q=0.9,en;q=0.8',
    'Appid': 'Reactweb',
    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Storeid': 'mafuae',
}

def get_categories(main_url, category_name, headers):

    response = requests.get(main_url, headers=headers)
    response_json = json.loads(response.text)

    categories_list = []

    for element in response_json:
        for parent in element.get('children', []):
            if parent['name'] == category_name:
                for child in parent.get('children', []):
                    for grand_child in child.get('children', []):
                        Category_L3_id = grand_child.get('categories.key')
                        product_count = grand_child.get('count')

                        category_dict = {
                            'category_id': Category_L3_id,
                            'product_count': product_count
                        }

                        categories_list.append(category_dict)

    return categories_list

def construct_url(category_id, product_count):
    # Base URL
    base_url = "https://www.carrefouruae.com/api/v8/categories/"

    # Relative path
    #relative_path = category_id

    # Query parameters
    params = {
        'currentPage': 0,
        'pageSize': 1, #product_count
        'lang': 'en',
        'displayCurr': 'AED',
        'latitude': 25.2171003,
        'longitude': 55.3613635
    }

    # Constructing the URL
    full_url = urljoin(base_url, category_id)
    full_url_with_params = full_url + '?' + urlencode(params)

    return full_url_with_params

def process_product_data(url, headers):
    response = requests.get(url, headers=headers)
    response_json = json.loads(response.text)
    
    clean_json = response_json['products']

    processed_data = []

    for product in clean_json:
        product_id = product['id']
        name = product['name']
        price = product['price']['price']

        try:
            discount_price = product['price']['discount']['price']
        except KeyError:
            discount_price = price

        product_info = {
            'product_id': product_id,
            'name': name,
            'price': price,
            'discount_price': discount_price
        }

        processed_data.append(product_info)

    return processed_data

main_url = 'https://www.carrefouruae.com/api/v1/menu?latitude=25.2171003&longitude=55.3613635&lang=en&displayCurr=AED'
category_name = "Electronics & Appliances"

result_categories = get_categories(main_url, category_name, global_headers)
for category_info in result_categories:
    category_id = category_info['category_id']
    product_count = category_info['product_count']
    url = construct_url(category_id, product_count)
    data = process_product_data(url, global_headers)
    print(data)