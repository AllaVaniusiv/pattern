import json
import os
from typing import Dict, Any


class Configuration:

    def __init__(self, config_path: str = None):

        if config_path is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            config_path = os.path.join(project_root, 'patternLab4', 'config', 'settings.json')

        with open(config_path, 'r', encoding='utf-8') as config_file:
            self.config = json.load(config_file)

    def get_dataset_url(self) -> str:
        return self.config.get('dataset_url', '')

    def get_max_rows(self) -> int:
        return self.config.get('max_rows', 100)

    def get_output_strategy(self) -> str:
        return self.config.get('output_strategy', 'console')

    def get_output_config(self, strategy: str = None) -> Dict[str, Any]:
        strategy = strategy or self.get_output_strategy()
        output_config = self.config.get('output_config', {})
        return output_config.get(strategy, {})
