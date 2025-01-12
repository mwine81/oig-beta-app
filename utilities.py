from pathlib import Path
import polars as pl
from polars import col as c
import plotly.express as px
import re

DATA = Path('__main__').parent / 'data/claims_clean.parquet'

interval_dict = {
  'Week':'1w',
   'Quarter':'1q',
   'Month':'1mo',
   'Year':'1y'
}


def get_product_list() -> list:
    return pl.scan_parquet(DATA).select(c.product).unique().sort('product').collect().to_series().to_list()

def get_provider_groups(product):
    return pl.scan_parquet(DATA).filter(c.dos.dt.date() > 2022).filter(c.pbm_id == 1).filter(c.product == product).group_by(c.group_name).len().filter(c.group_name.is_not_null()).filter(c.len > 2).unique().sort('group_name').collect().to_series().to_list()

def get_interval():
    return [i for i in interval_dict]

def filter_group(group):
    return c.icp_unit.filter(c.group_name == group).mean().alias(f'Avg {group} ICP')

icp_unit = (c.icp / c.qty).alias("icp_unit")
nadac_unit = (c.nadac/c.qty).alias("nadac_unit")

def generate_data_by_period(product, interval,filter_groups):
    data = pl.scan_parquet(DATA).filter(c.dos.dt.date() > 2022).filter(c.pbm_id == 1)
    return(
    data
    .filter(c.fee < 10)
    .filter(c.product == product)
    .with_columns(icp_unit,nadac_unit)
    .sort(by="dos")
    .group_by_dynamic(
        c.dos,
        every=interval_dict[interval],
        group_by='product'
    )
    .agg(
        c.icp_unit.mean().alias('Avg ICP'),
        c.nadac_unit.mean().alias('Avg NADAC'),
        c.icp_unit.filter(c.affiliated == False).mean().alias('Avg Non Affiliated ICP'),
        c.icp_unit.filter(c.affiliated == True).mean().alias('Avg Affiliated ICP'),
        *[filter_group(x) for x in filter_groups]
    )
    .with_columns(pl.lit(interval).alias('interval'))
    )

def load_data(product: str,interval: str,filter_groups:list, qty: float) -> pl.LazyFrame:
    data = generate_data_by_period(product,interval,filter_groups)
    pivot_on = [n for n in data.collect_schema().names() if re.match(r'(?i)avg',n)]
    return (data
            .with_columns(pl.col(pl.Float32).forward_fill())
            .unpivot(on=pivot_on, index=['dos','product'])
            .with_columns(c.value*qty)
            )

def create_figure(product,interval,filter_groups, qty):
    data = load_data(product,interval,filter_groups,qty).collect()
    fig = px.line(data,
                  x="dos",
                  y='value',
                  color='variable',
                  line_shape='spline',
                  width=1200,
                  height=600,
                  labels={'dos':'','value':'Avg ICP per Rx','variable':''},
                  title=f"Average Ingredient Cost per Rx for <br><b>{product}<b> x {qty} ct",
                    line_dash='variable',
                  )
    fig.update_layout(
        yaxis_tickformat = '$,.2f',
        title_subtitle_text=f"Express Scripts WV NADAC OIG Reporting 2023-2024 (Average by {interval})",
        plot_bgcolor='white',
        legend_x = .90,
        legend_y = 1.2,
    )
    fig.update_traces(opacity=0.60)
    fig.update_yaxes(
        ticksuffix = '      ',
        tickfont=dict(
            family="Arial",  # Choose a bold font family
            size=12,
            color="grey"),
        title_font_family='Arial black'
    )
    fig.update_xaxes(
        tickfont=dict(
            family="Arial",  # Choose a bold font family
            size=12,
            color="grey"),
    title_text = 'Interval',
    title_font_family='Arial black'
    )
    fig.update_traces(
        line=dict(width=3),
    )
    return fig


