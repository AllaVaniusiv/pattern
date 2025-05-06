import os
import sys
import argparse
from configuration import Configuration
from data_processor import DataProcessor
from output_strategy import OutputStrategyFactory


def main():
    parser = argparse.ArgumentParser(
        description='Обробник даних пожежних інцидентів з використанням паттерну Стратегія')
    parser.add_argument('--config', type=str, help='Шлях до конфігураційного файлу')
    parser.add_argument('--strategy', type=str, choices=['console', 'kafka', 'redis'],
                        help='Стратегія виводу (перевизначає значення з конфігурації)')
    parser.add_argument('--download-only', action='store_true',
                        help='Тільки завантажити дані без виводу')
    args = parser.parse_args()

    try:
        config_path = args.config
        if config_path and not os.path.exists(config_path):
            print(f"Помилка: Файл конфігурації не знайдено: {config_path}")
            return 1

        config = Configuration(config_path)

        dataset_url = config.get_dataset_url()
        max_rows = config.get_max_rows()

        output_strategy_type = args.strategy or config.get_output_strategy()

    except Exception as e:
        print(f"Помилка завантаження конфігурації: {e}")
        return 1

    data_processor = DataProcessor(dataset_url, max_rows)

    data_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                  'data', 'fire_incidents.csv')

    if not os.path.exists(data_file_path):
        print("Файл з даними не знайдено. Завантаження даних з віддаленого джерела...")
        if not data_processor.download_data(data_file_path):
            print("Не вдалося завантажити дані. Завершення роботи.")
            return 1

    if not data_processor.load_data(data_file_path):
        print("Не вдалося завантажити дані з файлу. Завершення роботи.")
        return 1

    if args.download_only:
        print("Дані успішно завантажені. Завершення роботи.")
        return 0

    data = data_processor.get_data()
    try:
        strategy_config = config.get_output_config(output_strategy_type)
        output_strategy = OutputStrategyFactory.create_strategy(output_strategy_type, strategy_config)
        output_strategy.output_data(data)
        output_strategy.close()

    except Exception as e:
        print(f"Помилка при виводі даних: {e}")
        return 1

    print("\nОбробка даних завершена успішно.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
