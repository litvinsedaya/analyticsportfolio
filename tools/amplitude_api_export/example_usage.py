#!/usr/bin/env python3
"""
Пример использования Amplitude API для выгрузки данных
Демонстрирует основные возможности пакета
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from amplitude.amplitude_client import AmplitudeClient


def example_basic_export():
    """Пример базовой выгрузки событий за вчера"""
    print("ПРИМЕР 1: Базовая выгрузка событий за вчера")
    print("-" * 50)
    
    try:
        client = AmplitudeClient()
        
        # Создание директории для результатов
        output_dir = project_root / "amplitude" / "exports"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Выгрузка за вчера
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime('%Y-%m-%d')
        output_file = output_dir / f"example_events_{date_str}.json"
        
        print(f"Выгружаем события за {date_str}")
        print(f"Сохраняем в {output_file}")
        
        result = client.get_yesterday_events(str(output_file))
        print(f"Выгрузка завершена: {result}")
        
    except Exception as e:
        print(f"Ошибка: {str(e)}")


def example_filtered_export():
    """Пример выгрузки с фильтрами"""
    print("\nПРИМЕР 2: Выгрузка с фильтрами")
    print("-" * 50)
    
    try:
        client = AmplitudeClient()
        
        # Создание директории для результатов
        output_dir = project_root / "amplitude" / "exports"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Выгрузка за конкретную дату с фильтром
        target_date = "2024-01-15"  # Замените на нужную дату
        output_file = output_dir / f"example_filtered_{target_date}.json"
        
        print(f"Выгружаем события за {target_date}")
        print(f"Фильтр: только события регистрации")
        print(f"Сохраняем в {output_file}")
        
        result = client.get_events_for_date_range(
            start_date=target_date,
            end_date=target_date,
            output_file=str(output_file),
            event_type="user_signed_up"  # Фильтр по типу события
        )
        print(f"Выгрузка завершена: {result}")
        
    except Exception as e:
        print(f"Ошибка: {str(e)}")


def example_custom_date_range():
    """Пример выгрузки за произвольный период"""
    print("\nПРИМЕР 3: Выгрузка за произвольный период")
    print("-" * 50)
    
    try:
        client = AmplitudeClient()
        
        # Создание директории для результатов
        output_dir = project_root / "amplitude" / "exports"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Выгрузка за последние 3 дня
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=2)
        
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        output_file = output_dir / f"example_range_{start_str}_to_{end_str}.json"
        
        print(f"Выгружаем события с {start_str} по {end_str}")
        print(f"Сохраняем в {output_file}")
        
        result = client.get_events_for_date_range(
            start_date=start_str,
            end_date=end_str,
            output_file=str(output_file)
        )
        print(f"Выгрузка завершена: {result}")
        
    except Exception as e:
        print(f"Ошибка: {str(e)}")


def main():
    """Запуск всех примеров"""
    print("ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ AMPLITUDE API")
    print("=" * 60)
    
    # Проверка наличия кредов
    if not os.getenv('AMPLITUDE_API_KEY') or not os.getenv('AMPLITUDE_SECRET_KEY'):
        print("ОШИБКА: Не найдены креды Amplitude в .env файле")
        print("Добавьте AMPLITUDE_API_KEY и AMPLITUDE_SECRET_KEY в .env")
        return
    
    # Запуск примеров
    example_basic_export()
    example_filtered_export()
    example_custom_date_range()
    
    print("\n" + "=" * 60)
    print("ВСЕ ПРИМЕРЫ ЗАВЕРШЕНЫ!")
    print("Проверьте папку amplitude/exports/ для результатов")
    print("=" * 60)


if __name__ == "__main__":
    main()
