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
    """Получить список продуктов из магазина Ozone.

    Args:
        last_id (str): ID последнего продукта
        client_id (str): Идентификатор клиента
        seller_token (str): Токен продавца для аутентификации запроса

    Returns:
        dict: Результат запроса API с информацией о продуктах.

    Examples:
        >>> get_product_list("123", "client_id_xyz", "token_abc")
        {'filter': {'offer_id': [], 'product_id': [], 'visibility': 'ALL'}, 'last_id': '', 'limit': 1000}

        >>> get_product_list("456", "invalid_client_id", "invalid_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: ...
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
    """Получить список артикулов из магазина Ozone.

    Args:
        client_id (str): Идентификатор клиента
        seller_token (str): Токен продавца

    Returns:
        list: Список артикулов

    Examples:
        >>> get_offer_ids("client_id_xyz", "token_abc")
        [123, 456, 789]

        >>> get_offer_ids("invalid_client_id", "invalid_token")
        []
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
    """Обновить цены на продукты.

    Args:
        prices (list): Список цен для обновления
        client_id (str): Идентификатор клиента
        seller_token (str): Токен продавца

    Returns:
        dict: Результат запроса API с информацией об обновлении цен.

    Examples:
        >>> update_price([{"offer_id": 123, "price": 25.99}], "client_id_xyz", "token_abc")
        {'status': 'OK', 'errors': []}

        >>> update_price([{"offer_id": 456, "price": 19.99}], "invalid_client_id", "invalid_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: ...
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
    """Обновить количество товаров на складе.

    Args:
        stocks (list): Список словарей, представляющих обновленные количества товаров на складе
        client_id (str): Идентификатор клиента
        seller_token (str): Токен продавца

    Returns:
        dict: Словарь, содержащий результат запроса API.

    Example:
        >>> update_stocks(updated_stocks, "client_id_xyz", "token_abc")
        {"result": [{"product_id": 55946, "offer_id": "PG-2404С1", "updated": true, "errors": []}]}

        >>> result = update_stocks(updated_stocks, "invalid_client_id", "invalid_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: ...
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
    """Скачать и извлечь информацию об остатках часов с интернет магазина Timeworld.

    Returns:
        list: Список словарей, представляющих информацию об остатках часов

    Examples:
        >>> download_stock()
        [{'Код': 'PG-2404С1', 'Наименование': 'Часы Casio', 'Количество': 50},
         {'Код': 'DW-5600BB', 'Наименование': 'Casio G-Shock', 'Количество': 30}]
        ...

        >>> download_stock()
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: ...
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
    """Создать список наличия товаров в магазине.

    Args:
        watch_remnants (list): Список словарей с информацией об остатках товаров.
        offer_ids (list): Список артикулов, которые уже загружены в магазин в Ozon.

    Returns:
        list: Список словарей с информацией о наличии товаров в магазине.

    Examples:
        >>> create_stocks([{"Код": "PG-2404С1", "Количество": 50}], ["PG-2404С1"])
        [{'offer_id': 'PG-2404С1', 'stock': 50}]

        >>> create_stocks("invalid_input", ["PG-2404С1"])
        Traceback (most recent call last):
            ...
        TypeError: ...
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
    """Создать список цен на товары в магазине.

    Args:
        watch_remnants (list): Список словарей с информацией об остатках товаров.
        offer_ids (list): Список артикулов, которые уже загружены в магазин.

    Returns:
        list: Список словарей с информацией о ценах на товары в магазине.

    Examples:
        >>> create_prices([{"Код": "PG-2404С1", "Цена": 199.99}], ["PG-2404С1"])
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': 'PG-2404С1', 'old_price': '0', 'price': 199.99}]

        >>> create_prices("invalid_input", ["PG-2404С1"])
        Traceback (most recent call last):
            ...
        TypeError: ...
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
    """Сконвертировать строку цены в числовой формат.

    Args:
        price (str): Строка, представляющая цену.

    Returns:
        str: Преобразованная строка с числовым форматом цены.

    Examples:
        >>> price_conversion("199.99")
        '199'

        >>> price_conversion("invalid_price")
        Traceback (most recent call last):
            ...
        ValueError: invalid literal for int() with base 10: 'invalid_price'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список на подсписки заданного размера.

    Args:
        lst (list): Исходный список.
        n (int): Размер подсписка.

    Yields:
        list: Подсписки заданного размера.

    Examples:
        >>> list(divide([1, 2, 3, 4, 5, 6, 7, 8, 9], 3))
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

        >>> list(divide("invalid_input", 3))
        Traceback (most recent call last):
            ...
        TypeError: ...
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
