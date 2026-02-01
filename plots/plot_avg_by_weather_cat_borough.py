from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import plotly.express as px


@dataclass
class TaxiChartsByBorough:
    """
    Analisi taxi normalizzata per record meteo,
    con breakdown per borough e condizioni meteo.
    """
    db_filename: str = "taxi_trips.duckdb"
    project_root: Optional[Path] = None
    schema: str = "dwh_datamart"

    def __post_init__(self) -> None:
        if self.project_root is None:
            self.project_root = Path(__file__).resolve().parents[1]
        self.db_path = (self.project_root / self.db_filename).resolve()

        if not self.db_path.exists():
            raise FileNotFoundError(f"DuckDB non trovato: {self.db_path}")

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path), read_only=True)

    # ------------------------------------------------------------------
    # LOADERS
    # ------------------------------------------------------------------

    def _load_df(self, sql: str, value_col: str, label_col: str) -> pd.DataFrame:
        with self._connect() as con:
            df = con.execute(sql).df()

        df[value_col] = df[value_col].astype(float)
        df[label_col] = df[label_col].astype(str)
        df["borough"] = df["borough"].astype(str)
        return df

    def load_rainy_by_borough(self) -> pd.DataFrame:
        sql = f"""
        WITH weather_totals AS (
            SELECT is_rainy, COUNT(DISTINCT key_weather) AS total_weather
            FROM {self.schema}.dm_weather_dt
            GROUP BY is_rainy
        ),
        taxi_totals AS (
            SELECT
                w.is_rainy,
                COUNT(*) AS taxi_trips,
                z.borough_name AS borough
            FROM {self.schema}.dm_fact_taxi_trip t
            JOIN {self.schema}.dm_weather_dt w ON w.key_weather = t.key_weather
            JOIN {self.schema}.dm_zone z ON z.key_zone = t.key_zone_pickup
            GROUP BY w.is_rainy, z.borough_name
        )
        SELECT
            t.is_rainy,
            t.taxi_trips,
            t.borough,
            CAST((t.taxi_trips * 1.0 / w.total_weather) AS DECIMAL(10,2)) AS trips_per_weather
        FROM taxi_totals t
        JOIN weather_totals w ON t.is_rainy = w.is_rainy
        ORDER BY borough;
        """
        df = self._load_df(sql, "trips_per_weather", "is_rainy")
        df["condition"] = df["is_rainy"].map({"true": "Rainy", "false": "Not rainy"})
        return df

    def load_snowy_by_borough(self) -> pd.DataFrame:
        sql = f"""
        WITH weather_totals AS (
            SELECT is_snowy, COUNT(DISTINCT key_weather) AS total_weather
            FROM {self.schema}.dm_weather_dt
            GROUP BY is_snowy
        ),
        taxi_totals AS (
            SELECT
                w.is_snowy,
                COUNT(*) AS taxi_trips,
                z.borough_name AS borough
            FROM {self.schema}.dm_fact_taxi_trip t
            JOIN {self.schema}.dm_weather_dt w ON w.key_weather = t.key_weather
            JOIN {self.schema}.dm_zone z ON z.key_zone = t.key_zone_pickup
            GROUP BY w.is_snowy, z.borough_name
        )
        SELECT
            t.is_snowy,
            t.taxi_trips,
            t.borough,
            CAST((t.taxi_trips * 1.0 / w.total_weather) AS DECIMAL(10,2)) AS trips_per_weather
        FROM taxi_totals t
        JOIN weather_totals w ON t.is_snowy = w.is_snowy
        ORDER BY borough;
        """
        df = self._load_df(sql, "trips_per_weather", "is_snowy")
        df["condition"] = df["is_snowy"].map({"true": "Snowy", "false": "Not snowy"})
        return df

    def load_intensity_by_borough(self, intensity_col: str) -> pd.DataFrame:
        sql = f"""
        WITH weather_totals AS (
            SELECT {intensity_col}, COUNT(DISTINCT key_weather) AS total_weather
            FROM {self.schema}.dm_weather_dt
            GROUP BY {intensity_col}
        ),
        taxi_totals AS (
            SELECT
                w.{intensity_col},
                z.borough_name AS borough,
                COUNT(*) AS taxi_trips
            FROM {self.schema}.dm_fact_taxi_trip t
            JOIN {self.schema}.dm_weather_dt w ON w.key_weather = t.key_weather
            JOIN {self.schema}.dm_zone z ON z.key_zone = t.key_zone_pickup
            GROUP BY w.{intensity_col}, z.borough_name
        )
        SELECT
            t.{intensity_col} AS intensity,
            t.taxi_trips,
            t.borough,
            CAST((t.taxi_trips * 1.0 / w.total_weather) AS DECIMAL(10,2)) AS trips_per_category
        FROM taxi_totals t
        JOIN weather_totals w ON t.{intensity_col} = w.{intensity_col}
        ORDER BY borough;
        """
        return self._load_df(sql, "trips_per_category", "intensity")

    def trip_distance_borough(self) -> pd.DataFrame:
        sql = f"""
        select z.borough_name , avg(f.trip_distance)
        from {self.schema}.dm_fact_taxi_trip f
        join {self.schema}.dm_zone z
            on f.key_zone_pickup = z.key_zone
        WHERE f.Airport_fee = 0
        GROUP BY z.borough_name
        """
        with self._connect() as con:
            df = con.execute(sql).df()

        # Adatta le colonne
        df = df.rename(columns={
            "borough_name": "borough",
            df.columns[1]: "trips_per_category",
        })
        df["intensity"] = "Avg trip distance (airport_fee=0)"

        df["borough"] = df["borough"].astype(str)
        df["intensity"] = df["intensity"].astype(str)
        df["trips_per_category"] = df["trips_per_category"].astype(float)

        return df

    # ------------------------------------------------------------------
    # PLOTS
    # ------------------------------------------------------------------

    def plot_by_borough_1(
        self,
        df: pd.DataFrame,
        x: str,
        y: str,
        color: str,
        title: str,
    ):
        fig = px.bar(
            df,
            x=x,
            y=y,
            color=color,
            barmode="group",
            text=y,
            title=title,
            template="simple_white",
        )

        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside", cliponaxis=False)
        fig.update_xaxes(title_text="Borough")
        fig.update_yaxes(type="log")
        fig.update_yaxes(title_text="Avg taxi trips per weather record")
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig

    def plot_by_borough_2(
            self,
            df: pd.DataFrame,
            x: str,
            y: str,
            color: str,
            title: str,
            category_order: Optional[list[str]] = None,
    ):
        d = df.copy()

        # Normalizza stringhe (evita mismatch "No Rain " vs "No Rain")
        d[color] = d[color].astype(str).str.strip()
        d[x] = d[x].astype(str).str.strip()

        # (Categorical + sort)
        if category_order is not None:
            d[color] = pd.Categorical(d[color], categories=category_order, ordered=True)
            d = d.sort_values([x, color])

            category_orders = {color: category_order}
        else:
            category_orders = None

        fig = px.bar(
            d,
            x=x,  # <-- usa il parametro x
            y=y,
            color=color,
            barmode="group",
            text=y,
            title=title,
            template="simple_white",
            category_orders=category_orders,
        )

        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside", cliponaxis=False)
        fig.update_xaxes(title_text="Borough")
        fig.update_yaxes(type="log")
        fig.update_yaxes(title_text="Avg taxi trips per weather record")
        fig.update_layout(title_x=0.5)

        fig.show()
        return fig

    def plot_by_trip_distance(
            self,
            df: pd.DataFrame,
            x: str,
            y: str,
            color: str,
            title: str,
            category_order: Optional[list[str]] = None,
    ):
        d = df.copy()

        d[color] = d[color].astype(str).str.strip()
        d[x] = d[x].astype(str).str.strip()

        category_orders = {color: category_order} if category_order is not None else None

        fig = px.bar(
            d,
            x=x,
            y=y,
            color=color,
            barmode="group",
            text=y,
            title=title,
            template="simple_white",
            category_orders=category_orders,
        )

        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside", cliponaxis=False)
        fig.update_xaxes(title_text="Borough")

        fig.update_yaxes(title_text="Avg trip distance (miles or km)")

        fig.update_layout(title_x=0.5)
        fig.show()
        return fig


# ------------------------------------------------------------------
# USO
# ------------------------------------------------------------------

if __name__ == "__main__":
    charts = TaxiChartsByBorough(db_filename="taxi_trips.duckdb")

    # Rainy vs Not Rainy by borough
    df_rainy = charts.load_rainy_by_borough()
    charts.plot_by_borough_1(
        df_rainy,
        x="borough",
        y="trips_per_weather",
        color="condition",
        title="Avg taxi trips per weather record by borough (rain vs no rain)",
    )

    # Snowy vs Not Snowy by borough
    df_snowy = charts.load_snowy_by_borough()
    charts.plot_by_borough_1(
        df_snowy,
        x="borough",
        y="trips_per_weather",
        color="condition",
        title="Avg taxi trips per weather record by borough (snow vs no snow)",
    )

    rain_order = ["No Rain", "Light Rain", "Moderate Rain", "Heavy Rain"]
    # Rain intensity by borough
    df_rain_int = charts.load_intensity_by_borough("rain_intensity")
    charts.plot_by_borough_2(
        df_rain_int,
        x="borough",
        y="trips_per_category",
        color="intensity",
        title="Avg taxi trips per weather record by rain intensity and borough",
        category_order=rain_order,
    )

    wind_order = ["No Wind", "Light wind", "Moderate Wind", "Strong Wind", "Very Strong Wind"]
    # Wind intensity by borough
    df_wind_int = charts.load_intensity_by_borough("wind_intensity")
    charts.plot_by_borough_2(
        df_wind_int,
        x="borough",
        y="trips_per_category",
        color="intensity",
        title="Avg taxi trips per weather record by wind intensity and borough",
        category_order=wind_order
    )

    snow_order = ["No Snow", "Light Snow", "Moderate Snow", "Heavy Snow"]
    # Snow intensity by borough
    df_snow_int = charts.load_intensity_by_borough("snow_intensity")
    charts.plot_by_borough_2(
        df_snow_int,
        x="borough",
        y="trips_per_category",
        color="intensity",
        title="Avg taxi trips per weather record by snow intensity and borough",
    )

    temp_order = ["Extreme Cold", "Freezing", "Cold", "Mild", "Warm", "Hot", "Extreme Heat" ]
    # Snow intensity by borough
    df_temp_int = charts.load_intensity_by_borough("temperature_category")
    charts.plot_by_borough_2(
        df_temp_int,
        x='borough',
        y="trips_per_category",
        color="intensity",
        title="Avg taxi trips per weather record by temperature intensity",
        category_order=temp_order
    )

    # Snow intensity by borough
    df_trip_distance = charts.trip_distance_borough()
    charts.plot_by_trip_distance(
        df_trip_distance,
        x="borough",
        y="trips_per_category",
        color="intensity",
        title="taxi trips per average distance (no airport 'La Guardia' and 'JFK' trips)",
    )


