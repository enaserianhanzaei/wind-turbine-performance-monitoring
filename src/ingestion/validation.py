REQUIRED_COLUMNS = {
    "timestamp": "datetime64[ns]",
    "turbine_id": "int64",
    "wind_speed": "float64",
    "wind_direction": "float64",
    "power_output": "float64",
}

SENSOR_COLUMNS = ["wind_speed", "wind_direction", "power_output"]

# Define the range of turbine IDs expected in each group
TURBINE_GROUPS = {
    1: (1, 5),
    2: (6, 10),
    3: (11, 15),
}
