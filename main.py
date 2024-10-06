import sys
from datetime import datetime
from utilss.dates_utils import Dates
from etls.bhavcopy_etls import BhavcopyExtraction # Ensure this matches the class name
from etls.yearly_high_low import Yearlydata
from etls.chartlinkk import ChartlinkExtraction
from data_transformer.data_transformation import BhavcopyTransformation,YearlyhighlowTransformation,ChartlinkTransformation,DataframeJoiner
from data_exporter.data_export import DataExporter
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')


if __name__ == "__main__":
    # Generate the list of dates using the Dates class
    end_date = datetime(2024, 10, 4)
    num_days = 1
    date_generator = Dates(end_date, num_days)
    working_days = date_generator.generate_previous_working_days()
    print(working_days)
    #sys.exit()
    # Process Bhavcopy data for the generated list of dates
    bhavcopy = BhavcopyExtraction()  # Ensure this matches the class name
    final_df_bhavcopy = bhavcopy.process_bhavcopy(working_days)

    year_high_low = Yearlydata()
    final_df_yearly = year_high_low.process_yearly_high_low(working_days)

    chartlink_extractor = ChartlinkExtraction()
    chartlink_df = chartlink_extractor.run_scans()
    print(chartlink_df.head())

    if not final_df_bhavcopy.empty or final_df_yearly.empty or chartlink_df.empty:
        # Transform the data
        transformer = BhavcopyTransformation(final_df_bhavcopy)
        transformed_df = transformer.bhavcopy_transformation()
        
        transformed_yearly = YearlyhighlowTransformation(final_df_yearly)
        transformed_data_yealy = transformed_yearly.Yearly_high_low_transformation()

        transformed_chartlink = ChartlinkTransformation(chartlink_df)
        mvr_data = transformed_chartlink.chartlink_Transformation()
        # Assuming 'transformed_df' is your main DataFrame and 'transformed_yealyHL_df' is the yearly high-low DataFrame
        dataframe_joiner = DataframeJoiner(transformed_df, transformed_data_yealy)
        final_df = dataframe_joiner.join_dataframes()

        # Create DataExporter instance
        exporter = DataExporter(output_path=None)
        exporter.save_to_excel(final_df, working_days)

        #Insert data into MySQL
        exporter.insert_into_mysql(final_df)
        exporter.insert_mvr_data(mvr_data)
    else:
        print("No data available for processing.")