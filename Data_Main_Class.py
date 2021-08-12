import os
from typing import Dict
import argparse


class DataIngestion():
    """This a base class. The beam pipeline 'string to bigquery row' step
    require input as a single csv line in string format. This module implement
    translate_csvline_todict func which does that. The single line input to this
    func is simulated by 'for line in lines'. This module just prints the dict 
    rows on console."""

    def __init__(self, schema_file_name: str, csv_file_name: str, input_dir: str):
        self.schema_file_name = schema_file_name
        self.csv_file_name = csv_file_name
        self.input_dir = input_dir

        current_dir = os.path.dirname(os.path.realpath(__file__))
        schema_file_path = os.path.join(
            current_dir, input_dir, schema_file_name)
        csv_file_path = os.path.join(current_dir, input_dir, csv_file_name)
        self.schema_file_path = schema_file_path
        self.csv_file_path = csv_file_path

    def get_schema_column_names(self) -> list:
        """reads schema json
        Returns:
            list: list of column names in the provided schema file
                example: ["state","gender","year","name","number","created_date"]
        """
        with open(self.schema_file_path) as f:
            schema_str = f.read()
            schema_list = eval(schema_str)
            return [i["name"] for i in schema_list]

    def translate_csvline_todict(self, csv_line: str) -> Dict[str, str]:
        """This method translates a single line of comma separated values to a
    dictionary which can be loaded into BigQuery

        Args:
            csv_line (str): A comma separated single csv line
                example: KS1,F1,1923,Dorothy1,654,11/28/2016

        Returns:
            Dict[str,str]: A dict mapping schema column names with the values from Arg csv_line
                example: {'state': 'KS1', 'gender': 'F1', 'year': '1923', 'name': 'Dorothy1', 'number': '654', 'created_date': '11/28/2016'}
        """
        list1 = self.get_schema_column_names()
        list2 = csv_line.split(",")
        combined_list = zip(list1, list2)
        single_row = dict(combined_list)
        return single_row


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--schema_filename',
                        required=False,
                        default="usa_names.json",
                        help='Name of the schema file')

    parser.add_argument('--csv_filename',
                        required=False,
                        default="usa_names.csv",
                        help='Name of the csv file')

    parser.add_argument('--files_dir',
                        required=False,
                        default="input",
                        help='Name of dir which contains the schema,csv files')

    known_args, pipeline_args = parser.parse_known_args()

    return known_args, pipeline_args


def main():
    known_args, pipeline_args = parse_args()
    data_injestion = DataIngestion(
        known_args.schema_filename, known_args.csv_filename, known_args.files_dir)

    with open(data_injestion.csv_file_path) as f:
        lines = f.readlines()
        for line in lines:
            print(data_injestion.translate_csvline_todict(line.strip()))


if __name__ == "__main__":
    main()
