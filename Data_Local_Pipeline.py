from apache_beam.io.textio import WriteToText
import Data_Main_Class
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class Data_ingestion_local_pipeline_subclass(Data_Main_Class.DataIngestion):
    """.."""

    def __init__(self, schema_file_name: str, csv_file_name: str, input_dir: str, output_dir: str = "output/out.txt"):
        super().__init__(schema_file_name, csv_file_name, input_dir)
        self.output_dir = output_dir


def main():
    known_args, pipeline_args = Data_Main_Class.parse_args()
    data_ingestion = Data_ingestion_local_pipeline_subclass(
        known_args.schema_filename, known_args.csv_filename, known_args.files_dir)

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    (p
     | 'Read From Text' >> beam.io.ReadFromText(data_ingestion.csv_file_path,
                                                skip_header_lines=0)
     | 'String to BigQuery Row' >>
     beam.Map(lambda s: data_ingestion.translate_csvline_todict(s))

     | 'Write to Local file' >> beam.io.WriteToText(data_ingestion.output_dir)
     )
    p.run().wait_until_finish()


if __name__ == "__main__":
    main()
