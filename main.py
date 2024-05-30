from bs4 import BeautifulSoup
import requests
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import re
import pyautogui
import json
import pandas as pd
from shapely.geometry import Point

def firts_twenty(url):
    response = requests.get(url)
    data = []
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'lxml')
        element =soup.find("script", {"nonce": True})
        if element:
            element_string=str(element)
            matches = re.finditer(r'{(.*?)}', element_string)
            for match in matches:
                json_object = match.group(1)
                if 'area' in json_object:
                    guid_match = re.search(r'guid:"(.*?)"', json_object)
                    if guid_match:
                        guid = guid_match.group(1)
                        data.append({'guid': guid})
        else:
            print("Элемент не найден")
    else:
        print("Error:", response.status_code)
    df = pd.DataFrame(data)
    df.to_csv('csv/firts_twenty.csv', index=False)

#получение HAR
def get_HAR():
    driver = webdriver.Edge(service=Service("drivers/msedgedriver.exe"))
    driver.get(base_url)
    time.sleep(2)
    driver.maximize_window()
    x = 450
    y = 200
    pyautogui.moveTo(x, y) # тыкаем в точку
    pyautogui.click()
    time.sleep(5)
    pyautogui.press('pagedown')
    # Получаем количество подгружаемых данных на странице
    items_count = len(driver.find_elements(By.CSS_SELECTOR, ".cursor-pointer"))
    # Прокручиваем страницу до тех пор, пока количество загруженных элементов не перестанет увеличиваться
    while True:
        pyautogui.press('pagedown')
        time.sleep(0.25)
        pyautogui.press('pagedown')
        time.sleep(1)
        # Получаем новое количество элементов
        new_items_count = len(driver.find_elements(By.CSS_SELECTOR, ".cursor-pointer"))
        if new_items_count == items_count:
            break
        items_count = new_items_count
    # Получаем все элементы
    items = driver.find_elements(By.CSS_SELECTOR, ".cursor-pointer")
    # Далее вручную нужно выгрузить HAR (Network -> Export HAR)
    time.sleep(600)
    driver.quit()


# Извлекаем текст из каждого элемента HAR и сохраняем файл как csv
def extract_text_from_har_and_save_to_csv(har_file_path, csv_file_path):
    with open(har_file_path, 'r', encoding='utf-8') as file:
        har_data = json.load(file)
    text_objects = []
    data_list = []
    for entry in har_data['log']['entries']:
        if 'response' in entry:
            response = entry['response']
            if 'content' in response:
                content = response['content']
                if 'text' in content:
                    text = content['text']
                    if 'objects' in text:
                        text_objects.append(text)
    for text in text_objects:
        try:
            data = json.loads(text)
            if isinstance(data, dict) and 'objects' in data:
                objects = data['objects']
                if isinstance(objects, list):
                    guids = [item['guid'] for item in objects]
                    data_list.extend(guids)
        except json.JSONDecodeError:
            pass
    df = pd.DataFrame(data_list, columns=['guid'])
    df.to_csv(csv_file_path, index=False)


#Парсим страницу
def parse_page(url, data_pd):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'lxml')
        element = soup.find(class_="absolute bottom-0 p-3 w-full")
        if element:
            # div_elements = soup.find(class_='col-12 lg:col-7')
            name = soup.find(class_="m-0 my-3 inline")
            new_data_df = pd.DataFrame({'name': [name.text], 'link': url, 'coordinates': [element.text]})
            data_pd = pd.concat([data_pd, new_data_df], ignore_index=True)
            div_elements = soup.find_all('div', class_='col-12 lg:col-7')
            for div in div_elements:
                span_elements = div.find_all('span', class_='text-sm text-500')
                for span_element in span_elements:
                    header = span_element.text.strip()
                    div_value_element = span_element.find_next_sibling('div')
                    if div_value_element:
                        value = div_value_element.text.strip()
                        if header not in data_pd.columns:
                            data_pd[header] = value
                        else:
                            data_pd.loc[len(data_pd) - 1, header] = value

            toggle_panels = soup.find_all(class_='toggle-panel')
            texts_2 = []
            # Проходимся по каждому toggle panel
            for panel in toggle_panels:
                # Находим заголовок h3 внутри текущего toggle panel
                header = panel.find('h3').text.strip()
                data_toggle = pd.DataFrame({header: [None]})

                 # Находим все элементы div с классом "mb-3" внутри текущего toggle panel
                mb3_elements = panel.find_all(class_='mb-3')

                # Проходимся по каждому mb-3 элементу и выводим текст span и div элементов
                for mb3_element in mb3_elements:
                    span_text = mb3_element.find('span', class_='text-sm text-500').text.strip()
                    div_text = mb3_element.find('div').text.strip()
                    texts_2.append(f"{span_text}: {div_text}")
                combined_text = '\n'.join(texts_2)
                data_toggle[header] = combined_text
                if header not in data_pd.columns:
                    data_pd[header] = data_toggle[header]
                else:
                    data_pd.loc[len(data_pd)-1, header] = combined_text
                combined_text = ""
                texts_2 = []
        else:
            print("Элемент не найден")
    else:
        print("Error:", response.status_code)
    return data_pd

def parse_all(df, base_url, output_file):
    #Создаем новый пустой датафрейм с колонками name link coordinates
    data_pd = pd.DataFrame({'name': [], 'link': [], 'coordinates': []})
    for guid in df['guid']:
        well_url = base_url + guid
        data_pd = parse_page(well_url, data_pd)
    data_pd.to_csv(output_file, index=False)
#Объединение первых 20 и всех остальных скважин
def combine_all(file_path_1,file_path_2, csv_file_path):
    df1 = pd.read_csv(file_path_1)
    df2 = pd.read_csv(file_path_2)
    result = pd.concat([df1, df2], ignore_index=True)
    result.to_csv(csv_file_path,index= False)
#Преобразование координат в точки WKT
def points(file_path, output_path):
    df = pd.read_csv(file_path)
    df[['longitude', 'latitude']] = df['coordinates'].str.split(',', expand=True).astype(float)
    df.drop(columns=['coordinates'], inplace=True)
    geometry = df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
    df.insert(2, 'WKT_Point', geometry.apply(lambda point: point.wkt))
    df.drop(columns=['longitude', 'latitude'], inplace=True)
    df.to_csv(output_path, index= False)
#Отображение глубины как float без м
def depth(file_path):
    df = pd.read_csv(file_path)
    depth = df['Глубина']
    depth_numeric = depth.str.replace(' м', '').str.replace(',', '.').astype(float)
    df['Глубина'] = depth_numeric
    df.to_csv(file_path, index=False)
def move_number_to_front(file_path):
    df = pd.read_csv(file_path)
    def _move_number_to_front(s):
        # Ищем все цифры в строке
        match = re.search(r'(\d+)', s)
        if match:
            number = match.group(1)
            # Удаляем цифры из строки и добавляем их в начало
            new_string = number + '-' + re.sub(r'\s*\d+\s*', '', s).strip()
            return new_string
        return s
    df['name'] = df['name'].apply(_move_number_to_front)
    df.to_csv(file_path, index=False)
def extract_organization(row):
    prefix = 'Организация, проводившая бурение: '
    if isinstance(row, str) and prefix in row:
        match = re.search(r'Организация, проводившая бурение: (.*?)(\n|$)', row)
        if match:
            return match.group(1).strip()
    return None
def remove_organization_text(row):
    prefix = 'Организация, проводившая бурение: '
    if isinstance(row, str) and prefix in row:
        return re.sub(r'Организация, проводившая бурение: .*?(\n|$)', '', row).strip()
    return row
def org_file(file_path):
    df = pd.read_csv(file_path)
    if 'Организация' not in df.columns:
        df.insert(11, 'Организация', df['Информация по бурению'].apply(extract_organization))
        df['Информация по бурению'] = df['Информация по бурению'].apply(remove_organization_text)
    df.to_csv(file_path, index=False)
def extract_actual_horizon(row):
    prefix = 'Фактический горизонт: '
    if isinstance(row, str) and prefix in row:
        match = re.search(r'Фактический горизонт: (.*?)(\n|$)', row)
        if match:
            return match.group(1).strip()
    return None
def remove_actual_horizon(row):
    prefix = 'Фактический горизонт: '
    if isinstance(row, str) and prefix in row:
        return re.sub(r'Фактический горизонт: .*?(\n|$)', '', row).strip()
    return row
def horizon_file(file_path):
    df = pd.read_csv(file_path)
    if 'Фактический горизонт' not in df.columns:
        df.insert(16, 'Фактический горизонт', df['Геологические задачи и результаты'].apply(extract_actual_horizon))
        df['Геологические задачи и результаты'] = df['Геологические задачи и результаты'].apply(remove_actual_horizon)
    df.to_csv(file_path, index=False)

base_url = "https://kern.vnigni.ru/well/catalog/"
# get_HAR()
# csv_file_path = 'csv/vnigni_guid.csv'
# extract_text_from_har_and_save_to_csv('HAR/kern.vnigni.ru.har', csv_file_path)
# df = pd.read_csv(csv_file_path)
# output_file = 'csv/vnigni.csv'
# # parse_all(df, base_url, output_file)
#firts_twenty(base_url)
# combine_all('csv/firts_twenty.csv','csv/vnigni.csv', 'csv/vnigni__full.csv')
# points('csv/vnigni__full.csv','csv/vnigni__full_points.csv')
# depth('csv/vnigni__full_points.csv')
# move_number_to_front('csv/vnigni__full_points.csv')
# df = pd.read_csv('csv/vnigni__full_points.csv')
org_file('csv/vnigni__full_4.csv')
horizon_file('csv/vnigni__full_4.csv')



