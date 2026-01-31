from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List

import duckdb
import pandas as pd
import plotly.express as px


@dataclass
class TaxiCharts:
    """
    Utility per estrarre dati da DuckDB (dbt marts) e creare grafici con Plotly.
    Pensata per uso in PyCharm: fig.show() apre una vista interattiva (browser o inline).
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

    # -------------------------
    # LOADERS (QUERY -> DF)
    # -------------------------

    def load_agg_rainy(self) -> pd.DataFrame:
        sql = f"""
        WITH weather_totals AS (
            SELECT
                is_rainy,
                COUNT(DISTINCT key_weather) AS total_weather
            FROM {self.schema}.dm_weather_dt
            GROUP BY is_rainy
        ),
        taxi_totals AS (
            SELECT
                w.is_rainy,
                COUNT(*) AS taxi_trips
            FROM {self.schema}.dm_fact_taxi_trip t
            JOIN {self.schema}.dm_weather_dt w
                ON w.key_weather = t.key_weather
            GROUP BY w.is_rainy
        )
        SELECT
            t.is_rainy,
            t.taxi_trips,
            CAST((t.taxi_trips * 1.0 / w.total_weather) AS DECIMAL(10,2)) AS trips_per_weather
        FROM taxi_totals t
        JOIN weather_totals w
            ON t.is_rainy = w.is_rainy
        ORDER BY t.is_rainy;
        """
        with self._connect() as con:
            df = con.execute(sql).df()

        df["trips_per_weather"] = df["trips_per_weather"].astype(float)
        df["label"] = df["is_rainy"].map({True: "Rainy", False: "Not rainy"})
        return df

    def load_agg_snowy(self) -> pd.DataFrame:
        sql = f"""
        WITH weather_totals AS (
            SELECT
                is_snowy,
                COUNT(DISTINCT key_weather) AS total_weather
            FROM {self.schema}.dm_weather_dt
            GROUP BY is_snowy
        ),
        taxi_totals AS (
            SELECT
                w.is_snowy,
                COUNT(*) AS taxi_trips
            FROM {self.schema}.dm_fact_taxi_trip t
            JOIN {self.schema}.dm_weather_dt w
                ON w.key_weather = t.key_weather
            GROUP BY w.is_snowy
        )
        SELECT
            t.is_snowy,
            t.taxi_trips,
            CAST((t.taxi_trips * 1.0 / w.total_weather) AS DECIMAL(10,2)) AS trips_per_weather
        FROM taxi_totals t
        JOIN weather_totals w
            ON t.is_snowy = w.is_snowy
        ORDER BY t.is_snowy;
        """
        with self._connect() as con:
            df = con.execute(sql).df()

        df["trips_per_weather"] = df["trips_per_weather"].astype(float)
        df["label"] = df["is_snowy"].map({True: "Snowy", False: "Not snowy"})
        return df

    def load_agg_rain_intensity(self) -> pd.DataFrame:
        sql = f"""
        WITH weather_totals AS (
            SELECT
                rain_intensity,
                COUNT(DISTINCT key_weather) AS total_weather_rain
            FROM {self.schema}.dm_weather_dt
            GROUP BY rain_intensity
        ),
        taxi_totals AS (
            SELECT
                w.rain_intensity,
                COUNT(*) AS taxi_trips
            FROM {self.schema}.dm_fact_taxi_trip t
            JOIN {self.schema}.dm_weather_dt w
                ON w.key_weather = t.key_weather
            GROUP BY w.rain_intensity
        )
        SELECT
            t.rain_intensity,
            t.taxi_trips,
            CAST((t.taxi_trips * 1.0 / w.total_weather_rain) AS DECIMAL(10,2)) AS trips_per_category
        FROM taxi_totals t
        JOIN weather_totals w
            ON t.rain_intensity = w.rain_intensity;
        """
        with self._connect() as con:
            df = con.execute(sql).df()

        df["trips_per_category"] = df["trips_per_category"].astype(float)
        # label leggibile (di solito già ok, ma uniformiamo)
        df["label"] = df["rain_intensity"].astype(str)
        return df

    def load_agg_wind_intensity(self) -> pd.DataFrame:
        sql = f"""
        WITH weather_totals AS (
            SELECT
                wind_intensity,
                COUNT(DISTINCT key_weather) AS total_weather_wind
            FROM {self.schema}.dm_weather_dt
            GROUP BY wind_intensity
        ),
        taxi_totals AS (
            SELECT
                w.wind_intensity,
                COUNT(*) AS taxi_trips
            FROM {self.schema}.dm_fact_taxi_trip t
            JOIN {self.schema}.dm_weather_dt w
                ON w.key_weather = t.key_weather
            GROUP BY w.wind_intensity
        )
        SELECT
            t.wind_intensity,
            t.taxi_trips,
            CAST((t.taxi_trips * 1.0 / w.total_weather_wind) AS DECIMAL(10,2)) AS trips_per_category
        FROM taxi_totals t
        JOIN weather_totals w
            ON t.wind_intensity = w.wind_intensity;
        """
        with self._connect() as con:
            df = con.execute(sql).df()

        df["trips_per_category"] = df["trips_per_category"].astype(float)
        df["label"] = df["wind_intensity"].astype(str)
        return df

    def load_agg_snow_intensity(self) -> pd.DataFrame:
        sql = f"""
        WITH weather_totals AS (
            SELECT
                snow_intensity,
                COUNT(DISTINCT key_weather) AS total_weather_snow
            FROM {self.schema}.dm_weather_dt
            GROUP BY snow_intensity
        ),
        taxi_totals AS (
            SELECT
                w.snow_intensity,
                COUNT(*) AS taxi_trips
            FROM {self.schema}.dm_fact_taxi_trip t
            JOIN {self.schema}.dm_weather_dt w
                ON w.key_weather = t.key_weather
            GROUP BY w.snow_intensity
        )
        SELECT
            t.snow_intensity,
            t.taxi_trips,
            CAST((t.taxi_trips * 1.0 / w.total_weather_snow) AS DECIMAL(10,2)) AS trips_per_category
        FROM taxi_totals t
        JOIN weather_totals w
            ON t.snow_intensity = w.snow_intensity;
        """
        with self._connect() as con:
            df = con.execute(sql).df()

        df["trips_per_category"] = df["trips_per_category"].astype(float)
        df["label"] = df["snow_intensity"].astype(str)
        return df

    # -------------------------
    # PLOTS
    # -------------------------

    @staticmethod
    def _apply_category_order(df: pd.DataFrame, label_col: str, order: Optional[List[str]]) -> pd.DataFrame:
        """Se fornisci un order, impone quell'ordine su label_col (utile per intensità)."""
        if order is None:
            return df
        df = df.copy()
        df[label_col] = pd.Categorical(df[label_col], categories=order, ordered=True)
        return df.sort_values(label_col)

    def plot_trips_per_weather_binary(
        self,
        df: pd.DataFrame,
        title: str,
        y_col: str = "trips_per_weather",
        label_col: str = "label",
    ):
        fig = px.bar(
            df,
            x=label_col,
            y=y_col,
            text=y_col,
            hover_data={
                y_col: ":.2f",
                "taxi_trips": True,
            },
            title=title,
            template="simple_white",
        )
        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside", cliponaxis=False)
        fig.update_xaxes(title_text="")
        fig.update_yaxes(title_text="Trips per weather record")
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig

    def plot_trips_per_category(
        self,
        df: pd.DataFrame,
        title: str,
        y_col: str = "trips_per_category",
        label_col: str = "label",
        order: Optional[List[str]] = None,
    ):
        df_plot = self._apply_category_order(df, label_col=label_col, order=order)

        fig = px.bar(
            df_plot,
            x=label_col,
            y=y_col,
            text=y_col,
            hover_data={
                y_col: ":.2f",
                "taxi_trips": True,
            },
            title=title,
            template="simple_white",
        )
        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside", cliponaxis=False)
        fig.update_xaxes(title_text="")
        fig.update_yaxes(title_text="Trips per weather record")
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig


if __name__ == "__main__":
    charts = TaxiCharts(db_filename="taxi_trips.duckdb")

    # 1) Rainy vs Not rainy
    df_rainy = charts.load_agg_rainy()
    charts.plot_trips_per_weather_binary(
        df_rainy,
        title="Avg taxi trips per weather record (rain vs no rain)",
    )

    # 2) Snowy vs Not snowy
    df_snowy = charts.load_agg_snowy()
    charts.plot_trips_per_weather_binary(
        df_snowy,
        title="Avg taxi trips per weather record (snow vs no snow)",
    )

    # 3) Rain intensity
    df_rain_int = charts.load_agg_rain_intensity()
    charts.plot_trips_per_category(
        df_rain_int,
        title="Avg taxi trips per weather record by rain intensity",
        order=["No Rain", "Light Rain", "Moderate Rain", "Heavy Rain"],
    )

    # 4) Wind intensity
    df_wind_int = charts.load_agg_wind_intensity()
    charts.plot_trips_per_category(
        df_wind_int,
        title="Avg taxi trips per weather record by wind intensity",
        order = ["No Wind", "Light Wind", "Moderate Wind", "Strong Wind", "Very Strong Wind"],
    )

    # 5) Snow intensity
    df_snow_int = charts.load_agg_snow_intensity()
    charts.plot_trips_per_category(
        df_snow_int,
        title="Avg taxi trips per weather record by snow intensity",
        # se hai categorie tipo: ["No Snow", "Light Snow", "Moderate Snow", "Heavy Snow"] ecc.
        order=None,
    )
