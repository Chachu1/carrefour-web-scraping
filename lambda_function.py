import json
import requests
from urllib.parse import urljoin, urlencode
from datetime import datetime
import boto3

s3 = boto3.client('s3')
current_date = datetime.now().strftime('%Y-%m-%d')
bucket = 'automated-web-scraping'


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
    # total_product_count = []

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
                        #total_product_count.append(product_count)

                        categories_list.append(category_dict)
    # print(sum(total_product_count))
    return categories_list

def construct_url(category_id, product_count):
    # Base URL
    base_url = "https://www.carrefouruae.com/api/v8/categories/"

    # Query parameters
    params = {
        'currentPage': 0,
        'pageSize': 1, #product_count,
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
        product_id = product.get('id')
        product_ean = product.get('ean')
        
        # Remove newline characters and white spaces from the 'name' variable
        name = product.get('name', '').replace('\n', '').strip()

        categories = product.get('category', [])
        Categorie_L1 = categories[0].get('name') if categories and len(categories) > 0 else None
        Categorie_L2 = categories[1].get('name') if categories and len(categories) > 1 else None
        Categorie_L3 = categories[2].get('name') if categories and len(categories) > 2 else None

        Brand = product.get('brand', {}).get('name')
        price = product.get('price', {}).get('price')
        seller = product.get('supplier')

        try:
            discount_price = product['price']['discount']['price']
        except KeyError:
            discount_price = price

        product_info = {
            'product_id': product_id,
            'product_ean': product_ean,
            'Categorie_L1': Categorie_L1,
            'Categorie_L2': Categorie_L2,
            'Categorie_L3': Categorie_L3,
            'name': name,
            'price': price,
            'discount_price': discount_price,
            'seller': seller
        }

        processed_data.append(product_info)

    return processed_data

def lambda_handler(event, context):
    main_url = 'https://www.carrefouruae.com/api/v1/menu?latitude=25.2171003&longitude=55.3613635&lang=en&displayCurr=AED'
    category_name = "Electronics & Appliances"
    
    try:
        result_categories = get_categories(main_url, category_name, global_headers)
    
        data_for_json = []
    
        for category_info in result_categories:
            category_id = category_info['category_id']
            product_count = category_info['product_count']
            url = construct_url(category_id, product_count)
            data = process_product_data(url, global_headers)
            data_for_json.extend(data)
    
        file_name = f'carrefour_UAE_{current_date}.json'
        uploadBytesStream = bytes(json.dumps(data_for_json, indent=2).encode('UTF-8'))
        s3.put_object(Bucket=bucket, Key=file_name, Body=uploadBytesStream)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'{file_name} uploaded'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }