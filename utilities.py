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
    return pl.scan_parquet(DATA).filter(c.product == product).group_by(c.group_name).len().filter(c.group_name.is_not_null()).filter(c.len > 2).unique().sort('group_name').collect().to_series().to_list()

def get_interval():
    return [i for i in interval_dict]

def filter_group(group):
    return c.icp_unit.filter(c.group_name == group).mean().alias(f'Avg {group} ICP')

icp_unit = (c.icp / c.qty).alias("icp_unit")
nadac_unit = (c.nadac/c.qty).alias("nadac_unit")


def generate_data_by_period(product, interval,filter_groups):
    data = pl.scan_parquet(DATA)
    return(
    data
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
                  labels={'dos':'','value':'Avg ICP per Rx','variable':''},
                  title=f"Average Ingredient Cost per Rx for <br><b>{product}<b> x {qty} ct",
                    line_dash='variable',
                  )
    fig.update_layout(
        yaxis_tickformat = '$,.0f',
        title_subtitle_text=f"Express Scripts WV NADAC OIG Reporting 2023-2024 (Average by {interval})",
        plot_bgcolor='white',
        legend_x = .90,
        legend_y = 1.2,
        hovermode='x unified'
    )
    fig.update_traces(
        opacity=0.60,
        #mode='lines+markers',
        hovertemplate='<b>%{y:$,.2f}<b>'
    )
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

CATEGORY_ORDER = {
    'variable': ['Rx Dispensed', 'Units Dispensed', 'Ingredient Cost Paid', 'NADAC', 'Margin Over NADAC']
}
REPLACEMENT_MAP = {
    'qty': "Units Dispensed",
    'rx_count': "Rx Dispensed",
    'icp': "Ingredient Cost Paid",
    'nadac': "NADAC",
    'margin': "Margin Over NADAC"
}
AFFILIATED_MAP = {'true': 'Affiliated', 'false': 'Non-Affiliated'}
COLOR_MAP = {'Affiliated': '#0077BE', 'Non-Affiliated': '#091851'}


# Helper function: Apply aggregations
def apply_aggregation(data):
    return data.group_by(c.affiliated).agg(
        pl.len().alias('rx_count'),
        c.nadac.sum(),
        c.icp.sum(),
        c.qty.sum()
    )

# Helper function: Apply column calculations
def apply_calculation(data):
    return data.with_columns(
        (c.icp - c.nadac).alias('margin')
    ).unpivot(index='affiliated').with_columns(
        (c.value / c.value.sum().over('variable')).alias('percent'),
        c.variable.replace(REPLACEMENT_MAP),
        c.affiliated.cast(pl.String).replace(AFFILIATED_MAP)
    ).sort(c.affiliated)


# Main function: Percent of total frame
def percent_of_total_frame(use_drug, product,specialty_selection,b_g):
    scanned_data = pl.scan_parquet(DATA)
    if use_drug == 'Selected Drug':
        scanned_data = scanned_data.filter(c.product == product)
    if specialty_selection == 'Specialty':
        scanned_data = scanned_data.filter(c.is_special)
    if specialty_selection == 'Non-Specialty':
        scanned_data = scanned_data.filter(c.is_special == False)
    if b_g == 1:
        scanned_data = scanned_data.filter(c.is_brand == True)
    if b_g == 0:
        scanned_data = scanned_data.filter(c.is_brand == False)

    aggregated_data = apply_aggregation(scanned_data)  # Apply aggregation
    return apply_calculation(aggregated_data).collect()  # Perform calculations


# Refactored function: Percent total figure
def percent_total_fig(data):
    fig = px.bar(
        data,
        x='percent',
        y='variable',
        color='affiliated',
        barmode='relative',
        text='value',
        title='Affiliated vs Non-Affiliated',
        color_discrete_map=COLOR_MAP,
        category_orders=CATEGORY_ORDER
    )
    fig.update_traces(
        textposition='inside',
        texttemplate='%{x:.0%}',
        textfont_size=13,
        width=0.75
    )
    fig.update_xaxes(showticklabels=False, title_text='')
    fig.update_yaxes(title_text='(Percent of Total)', ticksuffix=' ')
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title_text="")
    )
    return fig


