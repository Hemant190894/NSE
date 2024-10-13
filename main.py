import sys
import os
from datetime import datetime
import traceback
from utilss.dates_utils import Dates
from etls.bhavcopy_etls import BhavcopyExtraction
from etls.yearly_high_low import Yearlydata
from etls.chartlinkk import ChartlinkExtraction
from data_transformer.data_transformation import (BhavcopyTransformation, YearlyhighlowTransformation, ChartlinkTransformation, DataframeJoiner)
from data_exporter.data_export import DataExporter
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

if __name__ == "__main__":
    try:
        log_file_path = r"D:\New_Setup_Code\NSE\logs_file\logfile.txt"

        # Create log directory if it does not exist
        log_directory = os.path.dirname(log_file_path)
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)

        # Open log file
        with open(log_file_path, "a") as log_file:
            log_file.write(f"\n\n--- Script Started ---\n")
            log_file.write(f"Received arguments: {sys.argv}\n")

            # Get the date and number of days from arguments
            try:
                date_str = sys.argv[1]  # Start date ('2024-10-10')#
                num_days_str = sys.argv[2]  # Number of days
                log_file.write(f"Start Date: {date_str}\n")
                log_file.write(f"Number of Days: {num_days_str}\n")

                # Convert arguments to appropriate types
                end_date = datetime.strptime(date_str, '%Y-%m-%d')
                num_days = int(num_days_str)
            except (ValueError, IndexError) as ve:
                log_file.write(f"Error parsing arguments: {str(ve)}\n")
                log_file.write(f"Stack trace: {traceback.format_exc()}\n")
                sys.exit(1)

            # Generate working days
            date_generator = Dates(end_date, num_days)
            working_days = date_generator.generate_previous_working_days()
            log_file.write(f"Generated working days: {working_days}\n")

        # Proceed with data processing
        bhavcopy = BhavcopyExtraction()
        final_df_bhavcopy = bhavcopy.process_bhavcopy(working_days)

        year_high_low = Yearlydata()
        final_df_yearly = year_high_low.process_yearly_high_low(working_days)

        chartlink_extractor = ChartlinkExtraction()
        chartlink_df = chartlink_extractor.run_scans()

        if not final_df_bhavcopy.empty or final_df_yearly.empty or chartlink_df.empty:
            with open(log_file_path, "a") as log_file:
                log_file.write("Data available for processing.\n")

            # Transform the data
            transformer = BhavcopyTransformation(final_df_bhavcopy)
            transformed_df = transformer.bhavcopy_transformation()

            transformed_yearly = YearlyhighlowTransformation(final_df_yearly)
            transformed_data_yealy = transformed_yearly.Yearly_high_low_transformation()

            transformed_chartlink = ChartlinkTransformation(chartlink_df)
            mvr_data = transformed_chartlink.chartlink_Transformation()

            # Join dataframes
            dataframe_joiner = DataframeJoiner(transformed_df, transformed_data_yealy)
            final_df = dataframe_joiner.join_dataframes()

            # Create DataExporter instance
            exporter = DataExporter(output_path=None)
            exporter.save_to_excel(final_df, working_days)

            with open(log_file_path, "a") as log_file:
                log_file.write(f"Data Exported in CSV for {working_days[0]}.\n")

            # Insert data into MySQL
            exporter.insert_into_mysql(final_df)
            exporter.insert_mvr_data(mvr_data)

            with open(log_file_path, "a") as log_file:
                log_file.write(f"Data Inserted into MySQL for {working_days[0]}.\n")
        else:
            with open(log_file_path, "a") as log_file:
                log_file.write("No data available for processing.\n")

    except Exception as e:
        with open(log_file_path, "a") as log_file:
            log_file.write(f"Error occurred: {str(e)}\n")
            log_file.write(f"Stack trace: {traceback.format_exc()}\n")
