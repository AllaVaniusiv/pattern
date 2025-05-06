import os
import pandas as pd
import requests
from typing import List, Dict, Any


class DataProcessor:

    def __init__(self, dataset_url: str, max_rows: int = 100):
        self.dataset_url = dataset_url
        self.max_rows = max_rows
        self.data = None

    def download_data(self, output_file: str = None) -> bool:
        if output_file is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            output_file = os.path.join(project_root, 'patternLab4', 'data', 'fire_incidents.csv')

        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        try:
            print(f"Завантаження даних з {self.dataset_url}...")
            response = requests.get(self.dataset_url, stream=True)
            response.raise_for_status()

            with open(output_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Дані успішно завантажені та збережені у {output_file}")
            return True

        except Exception as e:
            print(f"Помилка при завантаженні даних: {e}")
            return False

    def load_data(self, file_path: str = None) -> bool:

        if file_path is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            file_path = os.path.join(project_root, 'patternLab4', 'data', 'fire_incidents.csv')

        try:
            self.data = pd.read_csv(file_path, nrows=self.max_rows)
            print(f"Завантажено {len(self.data)} рядків з {file_path}")
            return True

        except Exception as e:
            print(f"Помилка при завантаженні даних з файлу: {e}")
            return False

    def get_data(self) -> List[Dict[str, Any]]:
        if self.data is None:
            return []

        return self.data.to_dict('records')
