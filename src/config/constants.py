FORWARD_FILL_LIMIT_MINUTES = 10  # Max allowed gap for forward fill (in minutes)
REPORT_FREQ = 5  # Assuming data is every 5 minutes
OUTLIER_STD_THRESHOLD = 3  # z-score for outlier detection
IQR_FACTOR = 1.5

# Physical limits for each sensor feature
SENSOR_LIMITS = {
    "wind_speed": {
        "min": 0.0,     # cannot be negative
        "max": 100.0,   # implausibly high
    },
    "wind_direction": {
        "min": 0.0,     # degrees
        "max": 360.0,   # degrees
    },
    "power_output": {
        "min": 0.0,     # cannot be negative
        "max": None,    # no explicit upper bound
    },
}

# Define the database URL (SQLite in this case)
DATABASE_URL = "sqlite:///./wind_turbine_data.db"