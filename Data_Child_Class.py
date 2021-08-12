from typing import Dict
import Data_Main_Class


class DataIngestion_Subclass(Data_Main_Class.DataIngestion):
    """This module uses base class. It also just prints dict rows on the console.
    There is basic data transformation.."""

    def __init__(self, schema_file_name: str, csv_file_name: str, input_dir: str):
        super().__init__(schema_file_name, csv_file_name, input_dir)

    def translate_csvline_todict(self, csv_line: str) -> Dict[str, str]:
        """This method translates a single line of comma separated values to a
    dictionary which can be loaded into BigQuery

        Args:
            csv_line (str): A comma separated single csv line
                example: KS1,F1,1923,Dorothy1,654,11/28/2016

        Returns:
            Dict[str,str]: A dict mapping schema column names with the values from Arg csv_line 
            with year column transformed into YYYY-MM-DD format which BigQuery accepts
                example: {'state': 'KS1', 'gender': 'F1', 'year': '1923-01-01', 'name': 'Dorothy1', 'number': '654', 'created_date': '11/28/2016'}
        """
        list1 = self.get_schema_column_names()
        list2 = csv_line.split(",")
        year = "-".join((list2[2], "01", "01"))
        list2[2] = year
        combined_list = zip(list1, list2)
        single_row = dict(combined_list)
        return single_row


def main():
    known_args, pipeline_args = Data_Main_Class.parse_args()
    transformed_data_injestion = DataIngestion_Subclass(
        known_args.schema_filename, known_args.csv_filename, known_args.files_dir)
    with open(transformed_data_injestion.csv_file_path) as f:
        lines = f.readlines()
        for line in lines:
            print(transformed_data_injestion.translate_csvline_todict(line.strip()))


if __name__ == "__main__":
    main()
