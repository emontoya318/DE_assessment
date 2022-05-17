import pandas as pd
from datetime import datetime


def create_property_dataframe():
    """Create property dataframe with specified data types"""
    property_attributes_df = pd.DataFrame({'prop_id': pd.Series(dtype='str'),
                                           'prop_name': pd.Series(dtype='str'),
                                           'city': pd.Series(dtype='str'),
                                           'primary_use_type': pd.Series(dtype='str'),
                                           'property_sq_feet': pd.Series(dtype='str'),
                                           'floor_count': pd.Series(dtype='int'),
                                           'unit_count': pd.Series(dtype='int'),
                                           'net_rentable_area': pd.Series(dtype='int'),
                                           'gross_leasable_area': pd.Series(dtype='int'),
                                           'percent_occupied': pd.Series(dtype='float')
                                           })
    return property_attributes_df


def create_usage_dataframe():
    """Create usage dataframe with specified data types"""
    usage_and_consumption_df = pd.DataFrame({'index': pd.Series(dtype='int'),
                                             'prop_id': pd.Series(dtype='str'),
                                             'meter_id': pd.Series(dtype='str'),
                                             'start_date': pd.Series(dtype='int'),
                                             'end_date': pd.Series(dtype='int'),
                                             'monthly_usage': pd.Series(dtype='float'),
                                             'unit_of_measure': pd.Series(dtype='str'),
                                             'monthly_usage_converted': pd.Series(dtype='float'),
                                             'converted_unit': pd.Series(dtype='str')
                                             })
    return usage_and_consumption_df


def create_meter_dataframe():
    """Create meter dataframe with specified data types"""
    meter_data_df = pd.DataFrame({'meter_id': pd.Series(dtype='str'),
                                  'meter_type': pd.Series(dtype='str'),
                                  'simplified_meter_type': pd.Series(dtype='str'),
                                  'in_use': pd.Series(dtype='str'),
                                  'master_meter_v_submeter': pd.Series(dtype='str'),
                                  'inactive_date': pd.Series(dtype='int')
                                  })

    return meter_data_df


def clean_property(df):
    """clean dataframe by trimming and removing uppercase instances and removing duplicate on primary key (prop_id)"""
    # clean string inputs
    df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # drop duplicates and reset index
    df = df.drop_duplicates(subset=['prop_id'])
    df = df.reset_index(drop=True)
    return df


def clean_meter(df):
    """clean dataframe by trimming and removing uppercase instances and removing duplicate on primary key (meter_id)
    convert JSON timestamp(ms) format to datetime for date columns"""
    # clean string inputs
    df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # drop duplicates and reset index
    df = df.drop_duplicates(subset=['meter_id'])
    df = df.reset_index(drop=True)

    # drop rows which do not have any data
    df.dropna(how='all')

    # convert JSON timestamp(ms) to datetime
    df['inactive_date'] = df['inactive_date'] / 1000
    df['inactive_date'] = pd.to_datetime(df['inactive_date'], unit='s').dt.date

    return df


def clean_usage(df):
    """clean data by trimming and removing uppercase instances and removing duplicates usage charges on meter, units,
    and start_date - convert JSON timestamp(ms) format to datetime for date columns"""
    # clean string inputs
    df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # drop rows which do not have any data
    df.dropna(how='all')

    # drop rows with invalid JSON timestamp values (before year 2000)
    df.drop(df[df['start_date'] < 946702800000].index, inplace=True)
    df.drop(df[df['end_date'] < 946702800000].index, inplace=True)

    # drop duplicates and reset index
    df = df.drop_duplicates(subset=['meter_id', 'unit_of_measure', 'start_date'])
    df = df.reset_index(drop=True)

    # convert JSON timestamp(ms) to datetime
    df['start_date'] = df['start_date'] / 1000
    df['end_date'] = df['end_date'] / 1000
    df['start_date'] = pd.to_datetime(df['start_date'], unit='s').dt.date
    df['end_date'] = pd.to_datetime(df['end_date'], unit='s').dt.date

    return df


def rename_property(df):
    """ rename company property column names"""
    df = df.rename(columns={"Property id": "prop_id", "property name": "prop_name", "city__c": "city",
                            "primary_use_type__c": "primary_use_type", "property_sq_ft__c": "property_sq_feet",
                            "floor_count__c": "floor_count", "unit_count__c": "unit_count",
                            "net_rentable_area__c": "net_rentable_area",
                            "gross_leasable_area__c": "gross_leasable_area",
                            "percent_occupied__c": "percent_occupied", "new prop name": "prop_name"})

    return df


def rename_meter(df):
    """ rename company meter column names"""
    df = df.rename(columns={"included_in_metrics": "master_meter_v_submeter",
                            "simplified_meter_type": "simplified_meter_type", "inuse": "in_use",
                            "master_meter v submeter": "master_meter_v_submeter", "inactivedate": "inactive_date"})

    return df


def rename_usage(df):
    """ rename company usage column names"""
    df = df.rename(columns={"ID": "prop_id", "unitofmeasure": "unit_of_measure",
                            "monthly_usage_converted": "monthly_usage_converted",
                            "conversion_unit": "converted_unit"})
    return df
