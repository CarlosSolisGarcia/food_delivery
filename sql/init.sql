CREATE TABLE IF NOT EXISTS delivery_person (
    delivery_person_id VARCHAR(50) PRIMARY KEY,
    age INT,
    ratings FLOAT
);

CREATE TABLE IF NOT EXISTS restaurants (
    restaurant_id VARCHAR(50) PRIMARY KEY,
    restaurant_latitude FLOAT,
    restaurant_longitude FLOAT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    ordered_date TIMESTAMP,
    picked_date TIMESTAMP,
    weather_conditions VARCHAR(50),
    road_traffic_density VARCHAR(50),
    order_type VARCHAR(50),
    multiple_deliveries INT,
    festival BOOLEAN,
    city_type VARCHAR(50),
    time_taken_min INT,
    delivery_location_latitude FLOAT,
    delivery_location_longitude FLOAT,
    delivery_person_id VARCHAR(50),
    restaurant_id VARCHAR(50),
    vehicle_condition INT,
    vehicle_type VARCHAR(50),

    CONSTRAINT fk_delivery_person
        FOREIGN KEY (delivery_person_id)
        REFERENCES delivery_person(delivery_person_id),

    CONSTRAINT fk_restaurant
        FOREIGN KEY (restaurant_id)
        REFERENCES restaurants(restaurant_id)
);