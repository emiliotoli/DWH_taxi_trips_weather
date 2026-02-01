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

    def q_revenue_by_year_month(self) -> pd.DataFrame:
        sql = f"""
            SELECT
              d.year,
              EXTRACT(MONTH FROM d.date) AS month_num,
              d.month_name,
              SUM(f.total_amount) AS revenue
            FROM {self.schema}.dm_fact_taxi_trip f
            JOIN {self.schema}.dm_date d
              ON f.key_date_pickup = d.key_date
            GROUP BY d.year, month_num, d.month_name
            ORDER BY d.year, month_num
            """
        return self._sql(sql)

    # ----------------------------------------------------------------
    # PLOT
    # ----------------------------------------------------------------

    def plot_revenue_by_year_month(self, df: pd.DataFrame, title: str = "Revenue by year and month"):
        d = df.copy()

        month_order = (
            d.sort_values("month_num")["month_name"]
            .drop_duplicates()
            .tolist()
        )

        fig = px.line(
            d,
            x="month_name",
            y="revenue",
            color="year",
            markers=True,
            template="simple_white",
            title=title,
            category_orders={"month_name": month_order},
        )

        fig.update_yaxes(title_text="Revenue", tickformat="$~s")
        fig.update_xaxes(title_text="Month")
        fig.update_layout(title_x=0.5)
        fig.show()
        return fig


if __name__ == "__main__":
    charts = TaxiCharts(db_filename="taxi_trips.duckdb", schema="dwh_datamart")
    # 1) Revenue by year & month (line chart)
    df_rev = charts.q_revenue_by_year_month()
    charts.plot_revenue_by_year_month(df_rev)
