import os
import yaml
print(os.getcwd())


def load_config(file_path="config/config.yaml"):
    with open(file_path,"r") as file:
        return yaml.safe_load(file)
    
config = load_config()
print(config)