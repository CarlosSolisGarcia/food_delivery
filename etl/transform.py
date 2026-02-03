import pandas as pd
from datetime import datetime

def remove_trailing_spaces(df):
    """
    Remove trailing spaces from the columns of the dataframe.
    """
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    return df

def convert_delivery_person_id_to_string(df):
    """
    Convert the Delivery_person_ID column to a string.
    """
    df["Delivery_person_ID"] = df["Delivery_person_ID"].astype(str)
    return df


def convert_age_to_int(df):
    """
    Convert the Delivery_person_Age column to an integer and fill the missing values with the mean of the column.
    Accepts string values like 'NaN' in the CSV by converting to numeric with errors='coerce'.
    """
    s = pd.to_numeric(df["Delivery_person_Age"], errors="coerce")
    s = s.fillna(s.mean()).astype(int)
    df["Delivery_person_Age"] = s
    return df


def convert_ratings_to_float(df):
    """
    Convert the Delivery_person_Ratings column to a float and fill the missing values with the mean of the column.
    Also, ensure that the ratings are between 0 and 5. Accepts string values like 'NaN' in the CSV.
    """
    s = pd.to_numeric(df["Delivery_person_Ratings"], errors="coerce")
    s = s.clip(upper=5.0).fillna(s.mean().round(1))
    df["Delivery_person_Ratings"] = s
    return df


def unify_order_time(df):
    """
    Unify the order date and time ordered into a single column of type datetime.
    Expects Order_Date as DD-MM-YYYY and Time_Orderd as HH:MM:SS. Does not drop Order_Date
    so that unify_pickup_time can reuse it.
    """
    combined = df["Order_Date"].astype(str) + " " + df["Time_Orderd"].astype(str)
    df["Order_DateTime"] = pd.to_datetime(combined, format="%d-%m-%Y %H:%M:%S", errors="coerce")
    return df

def unify_pickup_time(df):
    """
    Unify the order date and time picked into a single column of type datetime,
    then drop the original date/time columns.
    Expects Order_Date as DD-MM-YYYY and Time_Order_picked as HH:MM:SS.
    """
    combined = df["Order_Date"].astype(str) + " " + df["Time_Order_picked"].astype(str)
    df["Order_Pickup_DateTime"] = pd.to_datetime(combined, format="%d-%m-%Y %H:%M:%S", errors="coerce")
    df = df.drop(columns=["Order_Date", "Time_Orderd", "Time_Order_picked"])
    return df

def convert_weather_conditions_to_string(df):
    """
    Convert the Weatherconditions column to a string. Removes the "conditions " prefix.
    """
    df["Weatherconditions"] = df["Weatherconditions"].astype(str)
    df["Weatherconditions"] = df["Weatherconditions"].str.replace("conditions ", "")
    return df

def convert_road_traffic_density_to_string(df):
    """
    Convert the Road_traffic_density column to a string.
    """
    df["Road_traffic_density"] = df["Road_traffic_density"].astype(str)
    return df

def convert_vehicle_condition_to_int(df):
    """
    Convert the Vehicle_condition column to an integer. Accepts string values like 'NaN' in the CSV.
    """
    s = pd.to_numeric(df["Vehicle_condition"], errors="coerce")
    df["Vehicle_condition"] = s.fillna(0).astype(int)
    return df

def convert_type_of_order_to_string(df):
    """
    Convert the Type_of_order column to a string.
    """
    df["Type_of_order"] = df["Type_of_order"].astype(str)
    return df

def convert_type_of_vehicle_to_string(df):
    """
    Convert the Type_of_vehicle column to a string.
    """
    df["Type_of_vehicle"] = df["Type_of_vehicle"].astype(str)
    return df

def convert_city_type_to_string(df):
    """
    Convert the City column to a string (dataset column is 'City').
    """
    df["City"] = df["City"].astype(str)
    return df

def convert_multiple_deliveries_to_int(df):
    """
    Convert the multiple_deliveries column to an integer. Accepts string values like 'NaN' in the CSV.
    """
    s = pd.to_numeric(df["multiple_deliveries"], errors="coerce")
    df["multiple_deliveries"] = s.fillna(0).astype(int)
    return df

def convert_festival_to_boolean(df):
    """
    Convert the Festival column to a boolean.
    """
    df["Festival"] = df["Festival"].astype(bool)
    return df

def convert_delivery_time_taken_min_to_int(df):
    """
    Convert the Time_taken(min) column to an integer and rename to Delivery_time_taken_min.
    Accepts string values like 'NaN' in the CSV.
    """
    s = df["Time_taken(min)"].str.replace("(min) ", "").str.strip()
    s = pd.to_numeric(s, errors="coerce")
    df["Delivery_time_taken_min"] = s.fillna(0).astype(int)
    df = df.drop(columns=["Time_taken(min)"])
    return df

def preprocess_data(df):
    """
    Preprocess the data by:
    - Removing trailing spaces
    - Converting the Delivery_person_id column to a string
    - Converting the Delivery_person_Age column to an integer
    - Converting the Delivery_person_Ratings column to a float
    - Unifying the order date and time ordered into a single column of type datetime
    - Unifying the order date and time picked into a single column of type datetime
    - Converting the Weatherconditions column to a string
    - Converting the Road_traffic_density column to a string
    - Converting the Vehicle_condition column to an integer
    - Converting the Type_of_order column to a string
    - Converting the Type_of_vehicle column to a string
    - Converting the City column to a string
    - Converting the multiple_deliveries column to an integer
    - Converting the Festival column to a boolean
    - Converting Time_taken(min) to integer as Delivery_time_taken_min
    - Returning the preprocessed dataframe
    """
    df = df.copy()
    df = remove_trailing_spaces(df)
    df = convert_delivery_person_id_to_string(df)
    df = convert_age_to_int(df)
    df = convert_ratings_to_float(df)
    df = unify_order_time(df)
    df = unify_pickup_time(df)
    df = convert_weather_conditions_to_string(df)
    df = convert_road_traffic_density_to_string(df)
    df = convert_vehicle_condition_to_int(df)
    df = convert_type_of_order_to_string(df)
    df = convert_type_of_vehicle_to_string(df)
    df = convert_city_type_to_string(df)
    df = convert_multiple_deliveries_to_int(df)
    df = convert_festival_to_boolean(df)
    df = convert_delivery_time_taken_min_to_int(df)
    return df
