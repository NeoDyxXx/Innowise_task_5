from airflow.models.baseoperator import BaseOperator
import pandas as pd
import numpy as np
import re

class CSVTransform(BaseOperator):
    def __init__(self, input_file_name: str, output_file_name:str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name

    def execute(self, context):
        data = pd.read_csv(self.input_file_name)
        data = data.fillna('-')
        data.content = data.content.apply(lambda x: re.sub(r'[^A-zА-я0-9,!\.?\s:]', '', str(x)\
            .replace(':)', '').replace(':D', '').replace(':(', '')))
        data.to_csv(self.output_file_name, sep=',', index=False)