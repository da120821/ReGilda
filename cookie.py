from selenium import webdriver
from selenium.webdriver.edge.service import Service
import json

# Путь к драйверу
edge_driver_path = r'Driver_Notes/msedgedriver.exe'

# Создаем экземпляр драйвера
service = Service(edge_driver_path)
driver = webdriver.Edge(service=service)

# Открываем сайт
driver.get('https://remanga.org')

# Ждем некоторое время, чтобы куки успели сформироваться
import time
time.sleep(60)

# Получаем все куки
all_cookies = driver.get_cookies()

# Фильтруем только нужные куки (оставляем только важные)
important_cookies = [
    cookie for cookie in all_cookies
    if cookie['name'] in ('serverUser', 'serverToken', 'sessionid2', 'user', 'token')
]

# Сохраняем куки в JSON-файл
with open('cookies.json', 'w') as file:
    json.dump(important_cookies, file, indent=4)
