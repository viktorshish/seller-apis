import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров для заданной кампании.

    Args:
        page (str): Значение токена страницы
        campaign_id (str): Идентификатор кампании
        access_token (str): Токен доступа к API

    Returns:
        list: Список товаров для кампании

    Examples:
        >>> get_product_list("token123", "campaign456", "access_token_xyz")
        [{'offerMappingEntries': [...], 'paging': {'nextPageToken': 'token456'}}]
        
        >>> get_product_list("", "invalid_campaign", "access_token_xyz")
        Traceback (most recent call last):
            ...
        requests.HTTPError: HTTP Error 404: Not Found
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить информацию о наличии товаров на складе.

    Args:
        stocks (list): Список записей о наличии товаров
        campaign_id (str): Идентификатор кампании
        access_token (str): Токен доступа к API

    Returns:
        dict: Объект ответа от API.

    Examples:
        >>> update_stocks([{"sku": "123", "warehouseId": "A", "items": [{"count": 10, "type": "FIT"}]}], "123", "token")
         {'status': 'OK', 'updatedCount': 1}
         
        >>> update_stocks([], "invalid_campaign", "token")
        Traceback (most recent call last):
            ...
        requests.HTTPError: HTTP Error 404: Not Found
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены.

    Args:
        prices (list): Список записей о ценах
        campaign_id (str): Идентификатор кампании
        access_token (str): Токен доступа к API

    Returns:
        dict: Объект ответа от API

    Examples:
        >>> update_price([{"id": "offer123", "price": {"value": 2599, "currencyId": "RUR"}}], "campaign456", "token_xyz")
        {'status': 'OK', 'updatedCount': 1}
        
        >>> update_price([], "invalid_campaign", "token_xyz")
        Traceback (most recent call last):
            ...
        requests.HTTPError: HTTP Error 404: Not Found
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета

    Args:
        campaign_id (str): Идентификатор компании
        market_token (str): Токен магазина

    Returns:
        list: Список товаров с артикулами

    Examples:
        >>> get_offer_ids("campaign123", "market_token_xyz")
        ['offer123', 'offer456', 'offer789']
        
        >>> get_offer_ids("campaign456", "invalid_token")
        Traceback (most recent call last):
        CustomException: Невозможно получить список предложений. Проверьте правильность токена доступа.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать список наличии товаров на складе.

    Args:
        watch_remnants(list): Список словарей с данными об остатках товаров
        offer_ids(list): Список артикулов
        warehouse_id (str): Идентификатор склада

    Returns:
        list: Список записей о наличии товаров на складе и цены

    Examples:
    >>> create_stocks([], ["offer123", "offer456"], "warehouse_ABC")
    [{'sku': 'offer123', 'warehouseId': 'warehouse_ABC', 'items': [{'count': 0, 'type': 'FIT', 'updatedAt': '2022-01-01T12:34:56Z'}]}, {'sku': 'offer456', 'warehouseId': 'warehouse_ABC', 'items': [{'count': 0, 'type': 'FIT', 'updatedAt': '2022-01-01T12:34:56Z'}]}]
    
    >>> create_stocks([], ["offer123", "offer456"], "invalid_warehouse")
    Traceback (most recent call last):
        ...
    CustomException: Невозможно создать записи о наличии товаров. Проверьте правильность идентификатора склада.
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать записи о ценах на основе данных об остатках и артикулах.

    Args:
        watch_remnants (list): Список данных об остатках товаров
        offer_ids (list): Список артикулов

    Returns:
        list: Список записей о ценах для артикулов

    Examples:
        >>> create_prices([{"Код": "offer123", "Цена": "25.99"}], ["offer123", "offer456"])
        [{'id': 'offer123', 'price': {'value': 2599, 'currencyId': 'RUR'}}]
        
        >>> create_prices([], ["offer123", "offer456"])
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Асинхронно загрузить цены.

    Args:
        watch_remnants (list): Список данных об остатках товаров
        campaign_id (str): Идентификатор кампании
        market_token (str): Токен доступа

    Returns:
        list: Список записей о ценах

    Examples:
        >>> await upload_prices([], "campaign123", "token_xyz")
        []
        
        >>> await upload_prices([{"Код": "offer123", "Цена": "25.99"}], "campaign456", "token_xyz")
        [{'id': 'offer123', 'price': {'value': 2599, 'currencyId': 'RUR'}}]
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
