"""
Load data into PostgreSQL: prepare DataFrames per table and insert.
"""
import os

import pandas as pd
import psycopg


def prepare_delivery_person_table(df):
    """DataFrame ready to insert into delivery_person."""
    df = df[["Delivery_person_ID", "Delivery_person_Age", "Delivery_person_Ratings"]].copy()
    df = df.drop_duplicates(subset=["Delivery_person_ID"], keep="first")
    df = df.rename(columns={
        "Delivery_person_ID": "delivery_person_id",
        "Delivery_person_Age": "age",
        "Delivery_person_Ratings": "ratings",
    })
    return df


def prepare_restaurants_table(df):
    """DataFrame ready to insert into restaurants."""
    df = df[["Restaurant_latitude", "Restaurant_longitude"]].copy()
    df = df.drop_duplicates(subset=["Restaurant_latitude", "Restaurant_longitude"])
    df["restaurant_id"] = [f"REST_{i}" for i in range(1, len(df) + 1)]
    df = df.rename(columns={
        "Restaurant_latitude": "restaurant_latitude",
        "Restaurant_longitude": "restaurant_longitude",
    })
    return df[["restaurant_id", "restaurant_latitude", "restaurant_longitude"]]


def prepare_orders_table(data, delivery_person_table, restaurants_table):
    """DataFrame ready to insert into orders; uses data and delivery/restaurant tables for FKs."""
    cols = [
        "ID", "Order_DateTime", "Order_Pickup_DateTime",
        "Weatherconditions", "Road_traffic_density", "Type_of_order",
        "multiple_deliveries", "Festival", "City",
        "Delivery_time_taken_min", "Delivery_location_latitude", "Delivery_location_longitude",
        "Delivery_person_ID", "Vehicle_condition", "Type_of_vehicle",
        "Restaurant_latitude", "Restaurant_longitude",
    ]
    df = data[cols].copy()
    df = df.merge(
        restaurants_table,
        left_on=["Restaurant_latitude", "Restaurant_longitude"],
        right_on=["restaurant_latitude", "restaurant_longitude"],
        how="left",
    )
    drop_cols = [
        "Restaurant_latitude", "Restaurant_longitude",
        "restaurant_latitude", "restaurant_longitude",
    ]
    df = df.drop(columns=drop_cols)
    df = df.rename(columns={
        "ID": "order_id",
        "Order_DateTime": "ordered_date",
        "Order_Pickup_DateTime": "picked_date",
        "Weatherconditions": "weather_conditions",
        "Road_traffic_density": "road_traffic_density",
        "Type_of_order": "order_type",
        "Festival": "festival",
        "City": "city_type",
        "Delivery_time_taken_min": "time_taken_min",
        "Delivery_location_latitude": "delivery_location_latitude",
        "Delivery_location_longitude": "delivery_location_longitude",
        "Delivery_person_ID": "delivery_person_id",
        "Vehicle_condition": "vehicle_condition",
        "Type_of_vehicle": "vehicle_type",
    })
    return df


def _connection_params():
    """Connection parameters from environment variables."""
    return {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
    }


def _rows_from_df(df, columns):
    """Convert a DataFrame to a list of tuples in the given column order."""
    return [
        tuple(row)
        for _, row in df[columns].iterrows()
    ]


def load_data(delivery_person_table, restaurants_table, orders_table):
    """Load data into the database tables."""
    dp_cols = ["delivery_person_id", "age", "ratings"]
    rest_cols = ["restaurant_id", "restaurant_latitude", "restaurant_longitude"]
    order_cols = [
        "order_id", "ordered_date", "picked_date", "weather_conditions",
        "road_traffic_density", "order_type", "multiple_deliveries", "festival",
        "city_type", "time_taken_min", "delivery_location_latitude",
        "delivery_location_longitude", "delivery_person_id", "vehicle_condition",
        "vehicle_type", "restaurant_id",
    ]

    insert_dp = (
        "INSERT INTO delivery_person (delivery_person_id, age, ratings) "
        "VALUES (%s, %s, %s)"
    )
    insert_rest = (
        "INSERT INTO restaurants (restaurant_id, restaurant_latitude, restaurant_longitude) "
        "VALUES (%s, %s, %s)"
    )
    insert_orders = (
        "INSERT INTO orders (order_id, ordered_date, picked_date, weather_conditions, "
        "road_traffic_density, order_type, multiple_deliveries, festival, city_type, "
        "time_taken_min, delivery_location_latitude, delivery_location_longitude, "
        "delivery_person_id, vehicle_condition, vehicle_type, restaurant_id) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    with psycopg.connect(**_connection_params()) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(insert_dp, _rows_from_df(delivery_person_table, dp_cols))
            cursor.executemany(insert_rest, _rows_from_df(restaurants_table, rest_cols))
            cursor.executemany(insert_orders, _rows_from_df(orders_table, order_cols))
        conn.commit()
