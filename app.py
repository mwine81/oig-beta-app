import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, callback, State
from utilities import get_product_list, get_interval, create_figure, get_provider_groups, percent_of_total_frame, \
    percent_total_fig

# Constants
DEFAULT_PRODUCT = 'Atorvastatin Calcium Oral Tablet 20 MG'
DEFAULT_INTERVAL = get_interval()[2]
DEFAULT_QUANTITY = 60


# Reusable function for dropdown cards
def create_dropdown_card(label, dropdown_id, options=[], default_value=None, multi=False, placeholder=None):
    return html.Div([
        dbc.Label(label, className="fw-bold"),
        dcc.Dropdown(
            options=options,
            value=default_value,
            id=dropdown_id,
            multi=multi,
            placeholder=placeholder,
        ),
    ])


# Components
product_dropdown = create_dropdown_card(
    label="Product Selection:",
    dropdown_id="product-dropdown",
    options=get_product_list(),
    default_value=DEFAULT_PRODUCT,
    placeholder="Select a product"
)

interval_dropdown = create_dropdown_card(
    label="Interval Selection:",
    dropdown_id="interval-dropdown",
    options=get_interval(),
    default_value=DEFAULT_INTERVAL,
)

provider_dropdown = create_dropdown_card(
    label="Choose Providers to Add:",
    dropdown_id="provider-dropdown",
    default_value=[],
    multi=True,
)

quantity_dropdown = create_dropdown_card(
    label="Quantity Selection:",
    dropdown_id="input-qty",
    options=[{"label": x, "value": x} for x in range(1000)],
    default_value=DEFAULT_QUANTITY,
)

controls = dbc.Card(
    [product_dropdown, interval_dropdown, provider_dropdown, quantity_dropdown],
    body=True,
)

fig_card = dbc.Card(dcc.Graph(id="main-graph"), className="mt-3")

agg_radio_group = html.Div(
    [
        dbc.RadioItems(
            id="agg-type",
            className="btn-group",
            inputClassName="btn-check",
            labelClassName="btn btn-outline-primary",
            labelCheckedClassName="active",
            options=[{"label": "All Drugs", "value": "All Drugs"},
                     {"label": "Selected Drug", "value": "Selected Drug"}],
            value="All Drugs",
        ),
        html.Div(id="output_agg-type"),
    ],
    className="radio-group",
)

specialty_radio_group = html.Div(
    [
        dbc.RadioItems(
            id="specialty",
            className="btn-group",
            inputClassName="btn-check",
            labelClassName="btn btn-outline-primary",
            labelCheckedClassName="active",
            options=[
                {"label": "All", "value": "All"},
                {"label": "Specialty", "value": "Specialty"},
                {"label": "Non Specialty", "value": "Non Specialty"},
            ],
            value="All",
        ),
        html.Div(id="output_specialty"),
    ],
    className="radio-group"
)

b_g_radio_group = html.Div(
    [
        dbc.RadioItems(
            id="b_g",
            className="btn-group",
            inputClassName="btn-check",
            labelClassName="btn btn-outline-primary",
            labelCheckedClassName="active",
            options=[
                {"label": "All", "value": "All"},
                {"label": "Brand", "value": 1},
                {"label": "Generic", "value": 0},
            ],
            value="All",
        ),
        html.Div(id="output_b_g"),
    ],
    className="radio-group"
)

agg_controls = dbc.Card(
    [
        dbc.Label("Aggregate Analysis:", className="fw-bold"),
        agg_radio_group,
        dbc.Label("Specialty Selection:", className="fw-bold"),
        specialty_radio_group,
        dbc.Label("Brand/Generic:", className="fw-bold"),
        b_g_radio_group,
    ],className='p-2'
)

collapse = html.Div(
    [
        dbc.Button(
            "Open collapse",
            id="horizontal-collapse-button",
            className="mb-3",
            color="primary",
            n_clicks=0,
        ),
        html.Div(
            dbc.Collapse(
                dbc.Card(
                    dbc.CardBody(
                        "This content appeared horizontally due to the "
                        "`dimension` attribute"
                    ),
                    style={"width": "400px"},
                ),
                id="horizontal-collapse",
                is_open=False,
                dimension="width",
            ),
            style={"minHeight": "100px"},
        ),
    ]
)




agg_fig_card = dbc.Card(dcc.Graph(id="agg-analysis"))

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# Layout
app.layout = dbc.Container(
    [
        html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(html.H1("WV Commercial Claims Data", className="text-white p-3 rounded mb-4",
                                        style={"backgroundColor": "#091851"}), width=9),
                        dbc.Col(html.Img(src="assets/final logo (1).png"), width=3, className="p-3"),
                    ],
                    justify="between",
                ),
                dbc.Row([dbc.Col(controls)]),
                dbc.Row([dbc.Col(fig_card)]),
                dbc.Row(
                    [
                        dbc.Col(agg_controls, width=4),
                        dbc.Col(agg_fig_card, width=8),
                    ],
                    className="mt-4 bg-light",
                ),
            ],
            className="bg-light p-3 rounded",
        )
    ],
    className="p-4",
    fluid=True,
)


# Callbacks
@callback(
    Output("main-graph", "figure"),
    Input("product-dropdown", "value"),
    Input("interval-dropdown", "value"),
    Input("provider-dropdown", "value"),
    Input("input-qty", "value"),
)
def update_fig(product, interval, provider, qty):
    return create_figure(product=product, interval=interval, filter_groups=provider, qty=qty)


@app.callback(
    Output("provider-dropdown", "options"),
    Input("product-dropdown", "value"),
)
def update_provider_options(product):
    return get_provider_groups(product)


@app.callback(
    Output("agg-analysis", "figure"),
    Input("agg-type", "value"),
    Input("product-dropdown", "value"),
    Input("specialty", "value"),
    Input("b_g", "value"),
)
def update_agg_fig(use_drug, product, specialty_selection,is_brand):
    data = percent_of_total_frame(use_drug, product, specialty_selection,is_brand)
    return percent_total_fig(data)


if __name__ == '__main__':
    app.run(debug=True)
