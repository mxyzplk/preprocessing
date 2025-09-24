import os
import yaml

class Config:
    def __init__(self):
        self.config = None
        self.modifications = None

        self.read_config()


    def read_config(self):

        main_dir = os.path.dirname(os.path.abspath(__file__))
        resources_dir = os.path.join(main_dir, '../resources')
        filepath = os.path.join(resources_dir,  'config.yaml')

        with open(filepath, "r") as f:
            
            self.config = yaml.safe_load(f)

        f.close()

        filepath = os.path.join(resources_dir,  'transformations.yaml')

        with open(filepath, "r") as f:
            
            self.transformations = yaml.safe_load(f)

        f.close()        
