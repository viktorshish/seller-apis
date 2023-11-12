# seller.py

Скрипт ``seller.py`` помогает автоматизировать размещение товара , такого как наручные часы на маркетплейсе OZON.

Для этого скрипт скачивает файл с актуальными товарами с сайта ``https://timeworld.ru``. Файл включает для каждой позиции: наименование товара, актуальные цену, актуальный остаток.

Затем данный файл обрабатывается и создается словарь с данными по каждой позиции товара, для дальнейшей передачи в OZON. 

Далее происходит размещение товара в магазине OZON. 

Если такой товар уже есть на платформе или цена отличается от цены взятой из скачанного файла, то обновляются его остатки или меняется его цена на OZON.