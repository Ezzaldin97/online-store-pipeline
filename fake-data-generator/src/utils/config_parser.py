import os
import yaml

class ConfigParser:
    def __init__(self) -> None:
        with open(os.path.join("conf", "credentials.yml")) as f:
            self.creds = yaml.safe_load(f)