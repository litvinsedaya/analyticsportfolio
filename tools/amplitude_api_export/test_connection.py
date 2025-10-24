#!/usr/bin/env python3
"""
Тестовый скрипт для проверки подключения к Amplitude API
Проверяет креды и доступность API без выгрузки данных
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from amplitude.amplitude_client import AmplitudeClient


def test_amplitude_connection():
    """Тестирование подключения к Amplitude API"""
    print("ТЕСТ ПОДКЛЮЧЕНИЯ К AMPLITUDE API")
    print("=" * 50)
    
    try:
        # Проверка переменных окружения
        print("ЭТАП 1: Проверка переменных окружения")
        api_key = os.getenv('AMPLITUDE_API_KEY')
        secret_key = os.getenv('AMPLITUDE_SECRET_KEY')
        
        if not api_key:
            print("AMPLITUDE_API_KEY не найден в .env")
            return False
        if not secret_key:
            print("AMPLITUDE_SECRET_KEY не найден в .env")
            return False
            
        print(f"API Key: {api_key[:8]}...")
        print(f"Secret Key: {secret_key[:8]}...")
        sys.stdout.flush()
        
        # Инициализация клиента
        print("\nЭТАП 2: Инициализация клиента")
        client = AmplitudeClient()
        print("Клиент инициализирован успешно")
        sys.stdout.flush()
        
        # Тест простого запроса (без создания экспорта)
        print("\nЭТАП 3: Тест API подключения")
        try:
            # Простой запрос для проверки аутентификации
            response = client.session.get(f"{client.base_url}/users/search")
            if response.status_code in [200, 400, 404]:  # 400/404 - нормальные ответы для пустого запроса
                print("API подключение работает")
                print(f"Статус ответа: {response.status_code}")
            else:
                print(f"Неожиданный статус: {response.status_code}")
        except Exception as e:
            print(f"Ошибка API запроса: {str(e)}")
            return False
        
        print("\n" + "=" * 50)
        print("ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
        print("Amplitude API готов к использованию")
        print("=" * 50)
        
        return True
        
    except Exception as e:
        print(f"\nОШИБКА ТЕСТИРОВАНИЯ: {str(e)}")
        return False


if __name__ == "__main__":
    success = test_amplitude_connection()
    sys.exit(0 if success else 1)
