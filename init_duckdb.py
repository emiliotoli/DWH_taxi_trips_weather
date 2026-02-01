import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / 'taxi_trips.duckdb'
DATA_DIR = BASE_DIR / 'data'


def init_duckdb():
    con = duckdb.connect(str(DB_PATH))
    con.execute('CREATE SCHEMA IF NOT EXISTS raw;')
    return con


def init_zones(con):
    con.execute(f"""
            CREATE OR REPLACE VIEW raw.borough AS
            SELECT *
            FROM read_csv_auto('{DATA_DIR / 'zones/borough.csv'}', header=True);
            """)

    con.execute(f"""
            CREATE OR REPLACE VIEW raw.neighborhood AS
            SELECT *
            FROM read_csv_auto('{DATA_DIR / 'zones/taxi_zone_lookup.csv'}', header=True);


        """)


def init_taxi_trips(con):
    con.execute(f"""
    CREATE OR REPLACE VIEW raw.taxi_trip AS
    SELECT *
    FROM read_parquet('{DATA_DIR / 'taxi_trip/*.parquet'}')

    """)


def init_weather(con):
    manhattan_path = (DATA_DIR / 'weather_dt/Weather_Manhattan/*.csv').as_posix()
    brooklyn_path = (DATA_DIR / 'weather_dt/Weather_Brooklyn/*.csv').as_posix()
    bronx_path = (DATA_DIR / 'weather_dt/Weather_Bronx/*.csv').as_posix()
    queens_path = (DATA_DIR / 'weather_dt/Weather_Queens/*.csv').as_posix()
    statenisland_path = (DATA_DIR / 'weather_dt/Weather_StatenIsland/*.csv').as_posix()
    ewr_path = (DATA_DIR / 'weather_dt/Weather_EWR/*.csv').as_posix()

    con.execute(f"""
    CREATE OR REPLACE VIEW raw.weather AS
    SELECT 
    'Bronx' as borough_name, 
    *
    FROM read_csv_auto('{bronx_path}', header=True , skip=3)
    UNION ALL
    SELECT 
    'Brooklyn' as borough_name, 
    *
    FROM read_csv_auto('{brooklyn_path}', header=True , skip=3)
    UNION ALL
    SELECT 
    'Manhattan' as borough_name, 
    *
    FROM read_csv_auto('{manhattan_path}', header=True , skip=3)
    UNION ALL
    SELECT 
    'Staten Island' as borough_name, 
    *
    FROM read_csv_auto('{statenisland_path}', header=True , skip=3)
    UNION ALL
    SELECT 
    'Queens' as borough_name, 
    *
    FROM read_csv_auto('{queens_path}', header=True , skip=3)
    UNION ALL
    SELECT 
    'EWR' as borough_name, 
    *
    FROM read_csv_auto('{ewr_path}', header=True , skip=3)
    """)


def init_files_dictionary(con):
    vendor_path = (DATA_DIR / 'taxi_trip/vendor_id.csv')
    ratecode_path = (DATA_DIR / 'taxi_trip/ratecode_id.csv')
    payment_type_path = (DATA_DIR / 'taxi_trip/payment_type.csv')
    con.execute(f"""
        CREATE OR REPLACE VIEW raw.vendor_id AS
        SELECT *
        FROM read_csv_auto('{vendor_path}', header=True)
        
    """)
    con.execute(f"""
            CREATE OR REPLACE VIEW raw.ratecode_id AS
            SELECT *
            FROM read_csv_auto('{ratecode_path}', header=True)

        """)
    con.execute(f"""
            CREATE OR REPLACE VIEW raw.payment_type AS
            SELECT *
            FROM read_csv_auto('{payment_type_path}', header=True)
        """)


if __name__ == "__main__":
    print("Inizio init_duckdb...")
    con = init_duckdb()
    init_zones(con)
    init_taxi_trips(con)
    init_weather(con)
    init_files_dictionary(con)
    print("Fine init_duckdb.")