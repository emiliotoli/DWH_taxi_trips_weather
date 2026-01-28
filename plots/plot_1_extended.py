from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import plotly.express as px


@dataclass
class TaxiCharts:
    """
    Utility per estrarre dati da DuckDB (dbt marts) e creare grafici con Plotly.

    - Si appoggia ad un file DuckDB (default: taxi_trips.duckdb) nel root progetto.
    - Tutte le query fanno riferimento allo schema `schema` (default: dwh_datamart).

    Nota:
    - Se alcune colonne differiscono nel tuo mart, modifica solo le query qui sotto.
    """
    db_filename: str = "taxi_trips.duckdb"
    project_root: Optional[Path] = None
    schema: str = "dwh_datamart"
    highlight_borough: str = "manhattan"

    def __post_init__(self) -> None:
        if self.project_root is None:
            # plots/qualcosa.py -> parents[1] = root progetto
            self.project_root = Path(__file__).resolve().parents[1]
        self.db_path = (self.project_root / self.db_filename).resolve()

        if not self.db_path.exists():
            raise FileNotFoundError(f"DuckDB non trovato: {self.db_path}")

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path), read_only=True)

    def _sql(self, query: str) -> pd.DataFrame:
        """Esegue SQL e ritorna un DataFrame."""
        with self._connect() as con:
            return con.execute(query).df()

    # ----------------------------
    # DATASETS (QUERY)
    # ----------------------------

    def q_weather_multidim(
        self,
        start_date: str = "2024-01-01",
        end_date: str = "2024-04-01",
    ) -> pd.DataFrame:
        """
        Query multidimensionale (borough pickup x day_name x weekend x season) con KPI.
        """
        sql = f"""
        SELECT
            -- Dimensioni
            zp.borough_name AS pickup_borough,
            d.day_name,
            d.is_weekend,
            d.season,

            -- Misure aggregate
            COUNT(*) AS total_trips,
            SUM(f.total_amount) AS total_revenue,
            AVG(f.total_amount) AS avg_fare,
            AVG(f.trip_distance) AS avg_distance,
            AVG(f.trip_duration_minutes) AS avg_duration,
            SUM(f.tip_amount) AS total_tips,

            -- Misure calcolate
            SUM(f.total_amount) / NULLIF(COUNT(*), 0) AS revenue_per_trip,
            SUM(f.trip_distance) / NULLIF(COUNT(*), 0) AS distance_per_trip
        FROM {self.schema}.dm_fact_taxi_trip f
        INNER JOIN {self.schema}.dm_zone zp
            ON f.key_zone_pickup = zp.key_zone
            AND zp.is_current = TRUE
        INNER JOIN {self.schema}.dm_date d
            ON f.key_date_pickup = d.key_date
        LEFT JOIN {self.schema}.dm_weather w
            ON f.key_weather = w.key_weather
        WHERE d.date >= '{start_date}'
          AND d.date <  '{end_date}'
        GROUP BY
            zp.borough_name,
            d.day_name,
            d.is_weekend,
            d.season
        """
        df = self._sql(sql)
        # Normalizzazioni utili
        df["pickup_borough"] = df["pickup_borough"].astype(str).str.lower()
        df["day_name"] = df["day_name"].astype(str)
        df["season"] = df["season"].astype(str)
        return df

    def q_dropoff_trips_by_neighborhood(self) -> pd.DataFrame:
        sql = f"""
        SELECT
            z.neighborhood_name,
            z.borough_name,
            COUNT(*) AS total_trips
        FROM {self.schema}.dm_fact_taxi_trip f
        JOIN {self.schema}.dm_zone z ON f.key_zone_dropoff = z.key_zone
        GROUP BY z.neighborhood_name, z.borough_name
        ORDER BY total_trips DESC
        """
        return self._sql(sql)

    def q_avg_revenue_by_vendor(self) -> pd.DataFrame:
        sql = f"""
        SELECT
            v.vendor_name,
            AVG(f.total_amount) AS avg_revenue
        FROM {self.schema}.dm_fact_taxi_trip f
        JOIN {self.schema}.dm_vendor v ON f.key_vendor = v.key_vendor
        GROUP BY v.vendor_name
        ORDER BY avg_revenue DESC
        """
        return self._sql(sql)

    def q_trips_by_apparent_temp_category(self) -> pd.DataFrame:
        sql = f"""
        SELECT
            w.apparent_temperature_category,
            COUNT(*) AS total_trips
        FROM {self.schema}.dm_fact_taxi_trip f
        JOIN {self.schema}.dm_weather w ON f.key_weather = w.key_weather
        GROUP BY w.apparent_temperature_category
        ORDER BY total_trips DESC
        """
        return self._sql(sql)

    def q_max_daily_revenue_january_2025(self) -> pd.DataFrame:
        sql = f"""
        WITH daily AS (
          SELECT
              d.date,
              SUM(f.total_amount) AS daily_revenue
          FROM {self.schema}.dm_fact_taxi_trip f
          JOIN {self.schema}.dm_date d ON f.key_date_pickup = d.key_date
          WHERE d.year = 2025
            AND d.month_name = 'january'
          GROUP BY d.date
        )
        SELECT
          d.date,
          d.daily_revenue
        FROM daily d
        WHERE d.daily_revenue = (
            SELECT MAX(daily_revenue)
            FROM daily
        )
        """
        return self._sql(sql)

    def q_revenue_by_year_month(self) -> pd.DataFrame:
        sql = f"""
        SELECT
          d.year,
          d.month_name,
          SUM(f.total_amount) AS revenue
        FROM {self.schema}.dm_fact_taxi_trip f
        JOIN {self.schema}.dm_date d ON f.key_date_pickup = d.key_date
        GROUP BY d.year, d.month_name
        ORDER BY d.year, d.month_name
        """
        return self._sql(sql)

    def q_christmas_day_trips_by_neighborhood_pu(self) -> pd.DataFrame:
        sql = f"""
        SELECT
          z.neighborhood_name,
          COUNT(*) AS trips
        FROM {self.schema}.dm_fact_taxi_trip f
        JOIN {self.schema}.dm_date d ON f.key_date_pickup = d.key_date
        JOIN {self.schema}.dm_zone z ON f.key_zone_pickup = z.key_zone
        WHERE d.is_holiday IS TRUE
          AND d.holiday_name = 'Christmas Day'
        GROUP BY z.neighborhood_name
        ORDER BY trips DESC
        """
        return self._sql(sql)

    def q_christmas_day_trips_by_neighborhood_do(self) -> pd.DataFrame:
        sql = f"""
        SELECT
          z.neighborhood_name,
          COUNT(*) AS trips
        FROM {self.schema}.dm_fact_taxi_trip f
        JOIN {self.schema}.dm_date d ON f.key_date_dropoff = d.key_date
        JOIN {self.schema}.dm_zone z ON f.key_zone_dropoff = z.key_zone
        WHERE d.is_holiday IS TRUE
          AND d.holiday_name = 'Christmas Day'
        GROUP BY z.neighborhood_name
        ORDER BY trips DESC
        """
        return self._sql(sql)

    def q_pickup_borough_season_temp(
        self,
        start_date: str = "2024-01-01",
        end_date: str = "2024-04-01",
    ) -> pd.DataFrame:
        sql = f"""
        SELECT
          zp.borough_name AS pickup_borough,
          d.season,
          w.apparent_temperature_category,
          COUNT(*) AS total_trips,
          SUM(f.total_amount) AS total_revenue,
          AVG(f.total_amount) AS avg_fare
        FROM {self.schema}.dm_fact_taxi_trip f
        JOIN {self.schema}.dm_zone zp
          ON f.key_zone_pickup = zp.key_zone
          AND zp.is_current = TRUE
        JOIN {self.schema}.dm_date d
          ON f.key_date_pickup = d.key_date
        LEFT JOIN {self.schema}.dm_weather w
          ON f.key_weather = w.key_weather
        WHERE d.date >= '{start_date}'
          AND d.date <  '{end_date}'
        GROUP BY
          zp.borough_name, d.season, w.apparent_temperature_category
        ORDER BY total_revenue DESC
        """
        return self._sql(sql)

    def q_tip_rate_by_temp(
        self,
        start_date: str = "2024-01-01",
        end_date: str = "2024-04-01",
    ) -> pd.DataFrame:
        sql = f"""
        SELECT
          w.apparent_temperature_category,
          COUNT(*) AS total_trips,
          SUM(f.tip_amount) AS total_tips,
          AVG(f.tip_amount) AS avg_tip,
          (SUM(f.tip_amount) / NULLIF(SUM(f.total_amount),0)) AS tip_rate
        FROM {self.schema}.dm_fact_taxi_trip f
        JOIN {self.schema}.dm_date d
          ON f.key_date_pickup = d.key_date
        LEFT JOIN {self.schema}.dm_weather w
          ON f.key_weather = w.key_weather
        WHERE d.date >= '{start_date}'
          AND d.date <  '{end_date}'
        GROUP BY w.apparent_temperature_category
        ORDER BY tip_rate DESC
        """
        return self._sql(sql)

    # ----------------------------
    # PLOTS
    # ----------------------------

    def plot_top_dropoff_neighborhoods(
        self,
        df: pd.DataFrame,
        top_n: int = 20,
        title: str = "Top dropoff neighborhoods by total trips",
    ):
        d = df.head(top_n).copy()
        d["label"] = d["neighborhood_name"].astype(str) + " (" + d["borough_name"].astype(str) + ")"
        fig = px.bar(
            d,
            x="total_trips",
            y="label",
            orientation="h",
            title=title,
            template="simple_white",
        )
        fig.update_yaxes(title_text="", categoryorder="total ascending")
        fig.update_xaxes(title_text="Total trips", tickformat="~s")
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig

    def plot_top_pickup_neighborhoods(
        self,
        df: pd.DataFrame,
        top_n: int = 10,
        title: str = "Top pickup neighborhoods by total trips",
    ):
        d = df.head(top_n).copy()
        d["label"] = d["neighborhood_name"].astype(str) + " (" + d["borough_name"].astype(str) + ")"
        fig = px.bar(
            d,
            x="total_trips",
            y="label",
            orientation="h",
            title=title,
            template="simple_white",
        )
        fig.update_yaxes(title_text="", categoryorder="total ascending")
        fig.update_xaxes(title_text="Total trips", tickformat="~s")
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig

    def plot_avg_revenue_by_vendor(
        self,
        df: pd.DataFrame,
        title: str = "Average revenue by vendor",
    ):
        fig = px.bar(
            df,
            x="vendor_name",
            y="avg_revenue",
            title=title,
            template="simple_white",
        )
        fig.update_yaxes(title_text="Avg revenue", tickformat="$~s")
        fig.update_xaxes(title_text="Vendor")
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig

    def plot_trips_by_temp_category(
        self,
        df: pd.DataFrame,
        title: str = "Trips by apparent temperature category",
    ):
        fig = px.bar(
            df,
            x="apparent_temperature_category",
            y="total_trips",
            title=title,
            template="simple_white",
        )
        fig.update_yaxes(title_text="Total trips", tickformat="~s")
        fig.update_xaxes(title_text="Apparent temperature category")
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig

    def plot_revenue_by_year_month(
        self,
        df: pd.DataFrame,
        title: str = "Revenue by year and month",
    ):
        # Crea un asse ordinabile anche senza conoscere l'ordine dei month_name
        d = df.copy()
        d["year"] = d["year"].astype(int)

        fig = px.line(
            d,
            x="month_name",
            y="revenue",
            color="year",
            markers=True,
            title=title,
            template="simple_white",
        )
        fig.update_yaxes(title_text="Revenue", tickformat="$~s")
        fig.update_xaxes(title_text="Month")
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig

    def plot_christmas_trips_top_neighborhoods_pu(
        self,
        df: pd.DataFrame,
        top_n: int = 10,
        title: str = "Christmas Day - top pickup neighborhoods by trips",
    ):
        d = df.head(top_n).copy()
        fig = px.bar(
            d,
            x="trips",
            y="neighborhood_name",
            orientation="h",
            title=title,
            template="simple_white",
        )
        fig.update_yaxes(title_text="", categoryorder="total ascending")
        fig.update_xaxes(title_text="Trips", tickformat="~s")
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig

    def plot_christmas_trips_top_neighborhoods_do(
        self,
        df: pd.DataFrame,
        top_n: int = 10,
        title: str = "Christmas Day - top dropoff neighborhoods by trips",
    ):
        d = df.head(top_n).copy()
        fig = px.bar(
            d,
            x="trips",
            y="neighborhood_name",
            orientation="h",
            title=title,
            template="simple_white",
        )
        fig.update_yaxes(title_text="", categoryorder="total ascending")
        fig.update_xaxes(title_text="Trips", tickformat="~s")
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig

    def plot_borough_season_temp_heatmap(
        self,
        df: pd.DataFrame,
        metric: str = "total_revenue",
        title: str = "Pickup borough x season x temp category",
    ):
        """
        Heatmap (facet per season) su una metrica (default total_revenue).
        """
        if metric not in df.columns:
            raise ValueError(f"Metrica '{metric}' non presente. Colonne: {list(df.columns)}")

        d = df.copy()
        d["pickup_borough"] = d["pickup_borough"].astype(str)
        d["season"] = d["season"].astype(str)
        d["apparent_temperature_category"] = d["apparent_temperature_category"].astype(str)

        fig = px.density_heatmap(
            d,
            x="apparent_temperature_category",
            y="pickup_borough",
            z=metric,
            facet_col="season",
            facet_col_wrap=2,
            title=f"{title} ({metric})",
            template="simple_white",
        )
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig

    def plot_tip_rate_by_temp(
        self,
        df: pd.DataFrame,
        title: str = "Tip rate by apparent temperature category",
    ):
        fig = px.bar(
            df,
            x="apparent_temperature_category",
            y="tip_rate",
            title=title,
            template="simple_white",
        )
        fig.update_yaxes(title_text="Tip rate (tips / total)", tickformat=".2%")
        fig.update_xaxes(title_text="Apparent temperature category")
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig


# ---- ESEMPIO USO (PyCharm: tasto destro -> Run) ----
if __name__ == "__main__":
    charts = TaxiCharts(db_filename="taxi_trips.duckdb", schema="dwh_datamart")

    # 0) Query multidimensionale principale (quella che avevi)
    df_agg = charts.q_weather_multidim(start_date="2024-01-01", end_date="2024-04-01")

    # 1) Top dropoff neighborhoods
    df_dropoff = charts.q_dropoff_trips_by_neighborhood()
    charts.plot_top_dropoff_neighborhoods(df_dropoff, top_n=20)

    # 2) Avg revenue per vendor
    df_vendor = charts.q_avg_revenue_by_vendor()
    charts.plot_avg_revenue_by_vendor(df_vendor)

    # 3) Trips per apparent temperature category
    df_temp = charts.q_trips_by_apparent_temp_category()
    charts.plot_trips_by_temp_category(df_temp)

    # 4) Max daily revenue in January 2025 (stampa tabella)
    df_max_day = charts.q_max_daily_revenue_january_2025()
    print("\nMax daily revenue - January 2025:")
    print(df_max_day)

    # 5) Revenue by year & month (line chart)
    df_rev = charts.q_revenue_by_year_month()
    charts.plot_revenue_by_year_month(df_rev)

    # 6) Christmas Day slice (top neighborhoods)
    df_xmas_do = charts.q_christmas_day_trips_by_neighborhood_do()
    charts.plot_christmas_trips_top_neighborhoods_do(df_xmas_do, top_n=10)
    df_xmas = charts.q_christmas_day_trips_by_neighborhood_pu()
    charts.plot_christmas_trips_top_neighborhoods_pu(df_xmas, top_n=10)

    # 7) Borough x season x temp category (heatmap)
    df_bst = charts.q_pickup_borough_season_temp(start_date="2024-01-01", end_date="2024-04-01")
    charts.plot_borough_season_temp_heatmap(df_bst, metric="total_revenue")

    # 8) Tip rate by temp category
    df_tip = charts.q_tip_rate_by_temp(start_date="2024-01-01", end_date="2024-04-01")
    charts.plot_tip_rate_by_temp(df_tip)
