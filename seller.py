import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Get the list of products of the ozone store.

    Args:
        last_id (str): The ID of the last product.
        client_id (str): The client ID for authenticating the request.
        seller_token (str): The seller token for authenticating the request.

    Returns:
        dict: A dictionary containing the result of the API request.

    Example:
        Correct Example:
        >>> print(get_product_list(last_id, client_id, seller_token))
        {"filter": {"offer_id": [],"product_id": [],"visibility": "ALL"},"last_id": "","limit": 100}

    Incorrect Example:
        >>> print(get_product_list(last_id, client_id, seller_token))
        {"code": 0,"details": [],"message": "string"}
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Get the offer IDs of products from the Ozone store.

    Args:
        client_id (str): The client ID for authenticating the request.
        seller_token (str): The seller token for authenticating the request.

    Returns:
        list: A list containing offer IDs of products.

    Example:
        >>> offer_ids = get_offer_ids(client_id, seller_token)
        >>> print(offer_ids)
        ['offer_id_1', 'offer_id_2', ...]

    Incorrect Example:
        >>> client_id = 123
        >>> seller_token = "your_seller_token"
        >>> offer_ids = get_offer_ids(client_id, seller_token)
        TypeError: Argument 'client_id' must be of type str.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Update the prices of products.

    Args:
        prices (list): A list of dictionaries representing the updated prices for products.
        client_id (str): The client ID for authenticating the request.
        seller_token (str): The seller token for authenticating the request.

    Returns:
        dict: A dictionary containing the result of the API request.

    Example:
        >>> result = update_price(updated_prices, client_id, seller_token)
        >>> print(result)
        {"result": [{"product_id": 1386, "offer_id": "PH8865", "updated": true, "errors": []}]}

    Incorrect Example:
        >>> result = update_price(updated_prices, client_id, seller_token)
        {"code": 0, "details": [{"typeUrl": "string", "value": "string"}], "message": "string"}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Update the stock quantities of products.

    Args:
        stocks (list): A list of dictionaries representing the updated stock quantities for products.
        client_id (str): The client ID for authenticating the request.
        seller_token (str): The seller token for authenticating the request.

    Returns:
        dict: A dictionary containing the result of the API request.

    Example:
        >>> result = update_stocks(updated_stocks, client_id, seller_token)
        >>> print(result)
        {"result": [{"product_id": 55946, "offer_id": "PG-2404С1", "updated": true, "errors": []}]}

    Incorrect Example:
        >>> result = update_stocks(updated_stocks, client_id, seller_token)
        >>> print(result)
        {  "code": 0, "details": [{"typeUrl": "string", "value": "string"}], "message": "string"}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Download file from the Casio website and return a list of watch remnants.

    Returns:
        list: A list of dictionaries representing watch remnants.

    Example:
        >>> watch_remnants = download_stock()
        >>> print(watch_remnants)
        [{'Watch Model': 'G-Shock GA-2100', 'Quantity': 50, 'Color': 'Black'}, ...]
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Create a list of stock quantities for the given watch remnants and offer IDs.

    Args:
        watch_remnants (List): A list of dictionaries representing watch remnants.
        offer_ids (List): A list of offer IDs.

    Returns:
        list: A list of dictionaries representing stock quantities.

    Example:
        >>> stocks = create_stocks(watch_remnants, offer_ids)
        >>> print(stocks)
        [{'offer_id': '123', 'stock': 100}, {'offer_id': '456', 'stock': 0}, ...]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Create a list of prices for the given watch remnants and offer IDs.

    Args:
        watch_remnants (List[Dict]]): A list of dictionaries representing watch remnants.
        offer_ids (List): A list of offer IDs.

    Returns:
        list: A list of dictionaries representing prices.

    Example:
        >>> prices = create_prices(watch_remnants, offer_ids)
        >>> print(prices)
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '123', 'old_price': '0', 'price': 199.99}, ...]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Return the converted price value.

    Convert the price by removing the signs, discarding the fractional part and the name of the money signs.

    Args:
        price (str): Price value

    Returns:
        str: Converted price with fractional part discarded and without the name of the monetary sign

    Examples:
        Correct Usage:
        >>> price_conversion("5'990.00 руб.")
        '5990'

    incorrect Examples:
        >>> price_conversion(5990.0)
        TypeError: Invalid input type...
        """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Divide the list 'lst' into parts of size 'n' elements.

    Args:
        lst (list): The input list to be divided.
        n (int): The number of elements in each part.

    Yields:
        list: A generator yielding parts of the input list.

    Example:
        >>> divided_parts = list(divide(input_list, 3))
        >>> print(divided_parts)
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

    Incorrect Example:
        >>> divided_parts = list(divide(input_list, '3'))
        >>> print(divided_parts)
        TypeError: 'str' object is not subscriptable
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
