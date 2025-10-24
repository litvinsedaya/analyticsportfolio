"""
Клиент для работы с Amplitude Export API
Скачивает данные напрямую в виде ZIP архива и конвертирует в CSV
"""

import os
import requests
import zipfile
import gzip
import json
from datetime import datetime, timedelta
from typing import Optional
import sys
from dotenv import load_dotenv
from tqdm import tqdm

# Загружаем переменные окружения из .env файла
load_dotenv()


class AmplitudeClient:
    """Клиент для работы с Amplitude Export API"""
    
    def __init__(self, show_progress: bool = True):
        """Инициализация клиента с кредами из .env"""
        self.api_key = os.getenv('AMPLITUDE_API_KEY')
        self.secret_key = os.getenv('AMPLITUDE_SECRET_KEY')
        self.show_progress = show_progress
        
        if not self.api_key or not self.secret_key:
            raise ValueError("Не найдены AMPLITUDE_API_KEY или AMPLITUDE_SECRET_KEY в .env файле")
        
        self.base_url = "https://amplitude.com/api/2"
        self.session = requests.Session()
        self.session.auth = (self.api_key, self.secret_key)
        
        if self.show_progress:
            print(f"Amplitude клиент инициализирован")
            print(f"API Key: {self.api_key[:8]}...")
            sys.stdout.flush()
    
    def download_events_zip(
        self, 
        start_date: str, 
        end_date: str,
        output_file: str,
        show_progress: bool = True
    ) -> str:
        """
        Скачать события из Amplitude в виде ZIP архива
        
        Args:
            start_date: Дата начала в формате YYYY-MM-DD
            end_date: Дата окончания в формате YYYY-MM-DD
            output_file: Путь к файлу для сохранения ZIP архива
            show_progress: Показывать прогресс-бар при скачивании (по умолчанию True)
            
        Returns:
            Путь к скачанному файлу
        """
        if self.show_progress:
            print(f"ЭТАП 2 из 4: Скачивание событий с {start_date} по {end_date}")
            sys.stdout.flush()
        
        # Конвертация дат в формат YYYYMMDDTHH
        start_formatted = start_date.replace('-', '') + 'T00'
        end_formatted = end_date.replace('-', '') + 'T23'
        
        # Формирование URL для экспорта
        url = f"{self.base_url}/export?start={start_formatted}&end={end_formatted}"
        
        if self.show_progress:
            print(f"URL: {url}")
            sys.stdout.flush()
        
        # Отправка запроса на экспорт
        response = self.session.get(url, stream=True)
        
        if response.status_code != 200:
            raise Exception(f"Ошибка скачивания экспорта: {response.status_code} - {response.text}")
        
        # Сохранение ZIP архива
        total_size = int(response.headers.get('content-length', 0))
        
        with open(output_file, 'wb') as f:
            if show_progress and total_size > 0:
                with tqdm(
                    desc=f"Скачивание {start_date}",
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            else:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        
        if self.show_progress:
            print(f"ZIP архив сохранен: {output_file}")
            print(f"Размер файла: {os.path.getsize(output_file)} байт")
            sys.stdout.flush()
        
        return output_file
    
    def extract_and_process_zip(
        self, 
        zip_file: str, 
        output_csv: str,
        show_progress: bool = True
    ) -> str:
        """
        Извлечь данные из ZIP архива и конвертировать в CSV
        
        Args:
            zip_file: Путь к ZIP архиву
            output_csv: Путь к выходному CSV файлу
            show_progress: Показывать прогресс-бар при обработке (по умолчанию True)
            
        Returns:
            Путь к CSV файлу
        """
        if self.show_progress:
            print(f"ЭТАП 3 из 4: Обработка ZIP архива {zip_file}")
            sys.stdout.flush()
        
        import pandas as pd
        
        # Создаем временную директорию для извлечения
        temp_dir = os.path.dirname(zip_file) + "/temp_extract"
        os.makedirs(temp_dir, exist_ok=True)
        
        try:
            # Извлекаем ZIP архив
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            if self.show_progress:
                print(f"Архив извлечен в: {temp_dir}")
                sys.stdout.flush()
            
            # Ищем JSON файлы в извлеченной директории
            json_files = []
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file.endswith('.json') or file.endswith('.gz'):
                        json_files.append(os.path.join(root, file))
            
            if not json_files:
                raise Exception("Не найдены JSON файлы в архиве")
            
            if self.show_progress:
                print(f"Найдено {len(json_files)} файлов для обработки")
                sys.stdout.flush()
            
            # Обрабатываем JSON файлы
            all_data = []
            if show_progress:
                json_files_iter = tqdm(json_files, desc="Обработка файлов")
            else:
                json_files_iter = json_files
                
            for json_file in json_files_iter:
                if self.show_progress:
                    print(f"Обработка файла: {os.path.basename(json_file)}")
                    sys.stdout.flush()
                
                try:
                    if json_file.endswith('.gz'):
                        # Обрабатываем сжатые файлы
                        with gzip.open(json_file, 'rt', encoding='utf-8') as f:
                            for line in f:
                                if line.strip():
                                    try:
                                        data = json.loads(line)
                                        all_data.append(data)
                                    except json.JSONDecodeError:
                                        continue
                    else:
                        # Обрабатываем обычные JSON файлы
                        with open(json_file, 'r', encoding='utf-8') as f:
                            for line in f:
                                if line.strip():
                                    try:
                                        data = json.loads(line)
                                        all_data.append(data)
                                    except json.JSONDecodeError:
                                        continue
                except Exception as e:
                    if self.show_progress:
                        print(f"Ошибка обработки файла {json_file}: {e}")
                    continue
            
            if not all_data:
                raise Exception("Не удалось извлечь данные из файлов")
            
            # Конвертируем в DataFrame и сохраняем в CSV
            df = pd.DataFrame(all_data)
            df.to_csv(output_csv, index=False, encoding='utf-8')
            
            if self.show_progress:
                print(f"ЭТАП 4 из 4: Данные сохранены в CSV: {output_csv}")
                print(f"Количество записей: {len(df)}")
                print(f"Колонки: {', '.join(df.columns.tolist())}")
                sys.stdout.flush()
            
            return output_csv
            
        finally:
            # Очищаем временную директорию
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    
    def get_events_for_date_range(
        self, 
        start_date: str, 
        end_date: str,
        output_dir: str,
        show_progress: bool = True
    ) -> str:
        """
        Полный цикл получения событий за период
        
        Args:
            start_date: Дата начала в формате YYYY-MM-DD
            end_date: Дата окончания в формате YYYY-MM-DD
            output_dir: Директория для сохранения файлов
            show_progress: Показывать прогресс-бары (по умолчанию True)
            
        Returns:
            Путь к CSV файлу с данными
        """
        if self.show_progress:
            print(f"ЭТАП 1 из 4: Инициализация выгрузки событий")
            print(f"Период: {start_date} - {end_date}")
            sys.stdout.flush()
        
        # Создаем директорию если не существует
        os.makedirs(output_dir, exist_ok=True)
        
        # Формируем имена файлов
        date_str = start_date.replace('-', '')
        zip_file = os.path.join(output_dir, f"amplitude_events_{date_str}.zip")
        csv_file = os.path.join(output_dir, f"amplitude_events_{date_str}.csv")
        
        # Скачиваем ZIP архив
        self.download_events_zip(start_date, end_date, zip_file, show_progress)
        
        # Обрабатываем архив и создаем CSV
        result_csv = self.extract_and_process_zip(zip_file, csv_file, show_progress)
        
        if self.show_progress:
            print(f"Выгрузка завершена успешно!")
            print(f"ZIP файл: {zip_file}")
            print(f"CSV файл: {result_csv}")
            sys.stdout.flush()
        
        return result_csv
    
    def get_yesterday_events(self, output_dir: str) -> str:
        """
        Получить события за вчерашний день
        
        Args:
            output_dir: Директория для сохранения файлов
            
        Returns:
            Путь к CSV файлу с данными
        """
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.strftime('%Y-%m-%d')
        end_date = yesterday.strftime('%Y-%m-%d')
        
        if self.show_progress:
            print(f"Получение событий за {start_date}")
            sys.stdout.flush()
        
        return self.get_events_for_date_range(start_date, end_date, output_dir)
