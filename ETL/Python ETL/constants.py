"""
    This module defines project-level constants
"""

# CSV_SOURCE_URL = 'https://www.football-data.co.uk/mmz4281/2324/E0.csv'
## !!! TEMP variable, let's use a local csv for testing purposes
CSV_SOURCE_URL = "../../dataset/PremierLeagueTESTDATA.csv"

CURRENT_SEASON_TAG = "2023-2024"
DEFAULT_MATCHWEEK = 1

# Database constants
DB_SERVER = "localhost"
DB_PORT = "5432"
DB_USER = "admin"
DB_PASSWORD = "root"
DB_NAME = "premier_league"