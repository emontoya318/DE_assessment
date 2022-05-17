import pandas as pd
import openpyxl
from plumbum import cli
from transform import *


class DataEngineeringAssessment(cli.Application):
    property_df = create_property_dataframe()
    usage_df = create_usage_dataframe()
    meter_df = create_meter_dataframe()

    def main(self):
        """Call to run utility bill ETL"""
        self.extract_property_data()
        self.extract_meter_data()
        self.extract_usage_date()
        self.write_to_excel()

    def extract_property_data(self):
        """Extract property data from JSON files to pandas df"""
        # read in property json files and load to df
        property_company1_df = pd.read_json('data/Company 1 Property Attributes.json')
        property_company2_df = pd.read_json('data/Company 2 Property Usage.json')
        property_company3_df = pd.read_json('data/Company 3 All Data.json')

        # rename property columns
        property_company1_df = rename_property(property_company1_df)
        property_company2_df = rename_property(property_company2_df)
        property_company3_df = rename_property(property_company3_df)

        # clean property df
        property_company1_df = clean_property(property_company1_df)
        property_company2_df = clean_property(property_company2_df)
        property_company3_df = clean_property(property_company3_df)

        # concat dataframes together on like columns and preventing property duplicates
        self.property_df = pd.concat([self.property_df, property_company1_df, property_company2_df,
                                      property_company3_df], join="inner", ignore_index=True)

    def extract_meter_data(self):
        """Extract meter data from JSON files to pandas df"""
        # read in meter json files and load to df
        meter_company1_df = pd.read_json('data/Company 1 Meter Usage.json')
        meter_company2_df = pd.read_json('data/Company 2 Property Usage.json')
        meter_company3_df = pd.read_json('data/Company 3 All Data.json')

        # rename meter columns
        meter_company1_df = rename_meter(meter_company1_df)
        meter_company2_df = rename_meter(meter_company2_df)
        meter_company3_df = rename_meter(meter_company3_df)

        # clean meter df
        meter_company1_df = clean_meter(meter_company1_df)
        meter_company2_df = clean_meter(meter_company2_df)
        meter_company3_df = clean_meter(meter_company3_df)

        # concat dataframes together on like columns and preventing meter duplicates
        self.meter_df = pd.concat([self.meter_df, meter_company1_df, meter_company2_df, meter_company3_df],
                                  join="inner", ignore_index=True)

    def extract_usage_date(self):
        """Extract usage data from JSON files to pandas df"""
        # read in usage data from JSON files to pandas df
        usage_company1_df = pd.read_json('data/Company 1 Meter Usage.json')
        usage_company2_df = pd.read_json('data/Company 2 Property Usage.json')
        usage_company3_df = pd.read_json('data/Company 3 All Data.json')

        # rename usage columns
        usage_company1_df = rename_usage(usage_company1_df)
        usage_company2_df = rename_usage(usage_company2_df)
        usage_company3_df = rename_usage(usage_company3_df)

        # clean usage df
        usage_company1_df = clean_usage(usage_company1_df)
        usage_company2_df = clean_usage(usage_company2_df)
        usage_company3_df = clean_usage(usage_company3_df)

        # concat dataframes together on like columns and preventing bill duplicates
        self.usage_df = pd.concat([self.usage_df, usage_company1_df, usage_company2_df, usage_company3_df],
                                  join="inner", ignore_index=True)

    def write_to_excel(self):
        """Write dataframe results to an Excel Workbook"""
        with pd.ExcelWriter('AssessmentFinal.xlsx') as writer:
            self.property_df.to_excel(writer, sheet_name='Property')
            self.meter_df.to_excel(writer, sheet_name='Meter')
            self.usage_df.to_excel(writer, sheet_name='Usage')


if __name__ == "__main__":
    try:
        sample_load = DataEngineeringAssessment()
    except Exception as e:
        print(e)
