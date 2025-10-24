#!/usr/bin/env python3
"""
Скрипт для выгрузки событий из Amplitude за сутки
Использует Amplitude Export API для получения данных
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm
import pandas as pd
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from amplitude.amplitude_client import AmplitudeClient


def setup_output_directory(show_progress: bool = True) -> Path:
    """Создание директории для сохранения данных"""
    if show_progress:
        print("ЭТАП 1 из 6: Настройка директории для данных")
        sys.stdout.flush()
    
    data_dir = project_root / "amplitude" / "exports"
    data_dir.mkdir(parents=True, exist_ok=True)
    
    if show_progress:
        print(f"Директория готова: {data_dir}")
        sys.stdout.flush()
    
    return data_dir


def export_events_for_date(
    date_str: str, 
    output_dir: Path,
    event_type: str = None,
    user_id: str = None,
    show_progress: bool = True
) -> str:
    """
    Выгрузка событий за конкретную дату
    
    Args:
        date_str: Дата в формате YYYY-MM-DD
        output_dir: Директория для сохранения
        event_type: Фильтр по типу события (опционально)
        user_id: Фильтр по пользователю (опционально)
        show_progress: Показывать прогресс-бары (по умолчанию True)
        
    Returns:
        Путь к файлу с данными
    """
    if show_progress:
        print(f"Начало выгрузки событий за {date_str}")
        sys.stdout.flush()
    
    # Инициализация клиента
    client = AmplitudeClient(show_progress=show_progress)
    
    # Выгрузка данных
    result_file = client.get_events_for_date_range(
        start_date=date_str,
        end_date=date_str,
        output_dir=str(output_dir),
        show_progress=show_progress
    )
    
    return result_file


def process_exported_data(csv_file: str, show_progress: bool = True) -> str:
    """
    Проверка обработанных данных
    
    Args:
        csv_file: Путь к CSV файлу с данными
        show_progress: Показывать прогресс-бары (по умолчанию True)
        
    Returns:
        Путь к CSV файлу
    """
    if show_progress:
        print(f"ЭТАП 6 из 6: Проверка данных из {csv_file}")
        sys.stdout.flush()
    
    # Чтение CSV данных для проверки
    df = pd.read_csv(csv_file, low_memory=False)
    
    if df.empty:
        if show_progress:
            print("Данные не найдены в файле")
            sys.stdout.flush()
        return csv_file
    
    if show_progress:
        print(f"Данные проверены и готовы к использованию")
        print(f"Количество записей: {len(df)}")
        print(f"Колонки: {', '.join(df.columns.tolist())}")
        sys.stdout.flush()
    
    return csv_file


def main():
    """Основная функция скрипта"""
    parser = argparse.ArgumentParser(description='Выгрузка событий из Amplitude за сутки')
    parser.add_argument(
        '--date', 
        type=str, 
        help='Дата в формате YYYY-MM-DD (по умолчанию - вчера)',
        default=None
    )
    parser.add_argument(
        '--event-type', 
        type=str, 
        help='Фильтр по типу события',
        default=None
    )
    parser.add_argument(
        '--user-id', 
        type=str, 
        help='Фильтр по ID пользователя',
        default=None
    )
    parser.add_argument(
        '--output-dir', 
        type=str, 
        help='Директория для сохранения (по умолчанию - data/amplitude_exports)',
        default=None
    )
    parser.add_argument(
        '--no-progress', 
        action='store_true', 
        help='Отключить прогресс-бары (для продакшна)',
        default=False
    )
    
    args = parser.parse_args()
    
    # Определение даты
    if args.date:
        target_date = args.date
    else:
        yesterday = datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime('%Y-%m-%d')
    
    show_progress = not args.no_progress
    
    if show_progress:
        print("=" * 60)
        print("AMPLITUDE EVENTS EXPORT")
        print("=" * 60)
        print(f"Дата: {target_date}")
        if args.event_type:
            print(f"Тип события: {args.event_type}")
        if args.user_id:
            print(f"Пользователь: {args.user_id}")
        print("=" * 60)
        sys.stdout.flush()
    
    try:
        # Настройка директории
        if args.output_dir:
            output_dir = Path(args.output_dir)
        else:
            output_dir = setup_output_directory(show_progress)
        
        # Выгрузка данных
        json_file = export_events_for_date(
            date_str=target_date,
            output_dir=output_dir,
            event_type=args.event_type,
            user_id=args.user_id,
            show_progress=show_progress
        )
        
        # Обработка данных
        csv_file = process_exported_data(json_file, show_progress)
        
        if show_progress:
            print("\n" + "=" * 60)
            print("ВЫГРУЗКА ЗАВЕРШЕНА УСПЕШНО!")
            print("=" * 60)
            print(f"JSON файл: {json_file}")
            print(f"CSV файл: {csv_file}")
            print("=" * 60)
        
    except Exception as e:
        if show_progress:
            print(f"\nОШИБКА: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
