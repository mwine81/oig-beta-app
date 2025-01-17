import dash
import dash_bootstrap_components as dbc
from dash import html, dcc,Input, Output, callback
from utilities import *

app = dash.Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

product_dd = html.Div([
            dbc.Label('Product Selection:'),
            dcc.Dropdown(
                options=get_product_list(),
                value='Atorvastatin Calcium Oral Tablet 20 MG',
                id='product-dropdown',
                placeholder='Select a product'
            )])

interval_dd = html.Div([
            dbc.Label('Interval Selection:'),
            dcc.Dropdown(
                options=get_interval(),
                value=get_interval()[2],
                id='interval-dropdown',
            )])
provider_dd =html.Div( [
            dbc.Label('Chose Providers to add:'),
            dcc.Dropdown(value=[],multi=True,id='provider-dropdown')
        ])

quantity_selection = html.Div([
    dbc.Label('Quantity Selection:'),
    dcc.Dropdown([x for x in range(1000)],
                value=60,
                id='input-qty',)
    ])

controls = dbc.Card([
    html.Div(product_dd),
    html.Div(interval_dd),
    html.Div(provider_dd),
    html.Div(quantity_selection)],
    body=True,
)


controls = dbc.Card(
    [provider_dd,interval_dd,product_dd,quantity_selection],
    body=True,
)

fig_layout = dbc.Card(dcc.Graph(id='main-graph'), className="mt-3")


app.layout = dbc.Container([
    html.Div([
    dbc.Row([
    dbc.Col(html.H1('WV Commercial Claims Data',className="text-white p-3 rounded mb-4", style={'background-color':'#091851'}),width=9),
        dbc.Col(html.Img(src='assets/final logo (1).png'), width=3,className='p-3')
    ],justify="between"),
    dbc.Row([
        dbc.Col(controls)
    ]),
    dbc.Row([
        dbc.Col([fig_layout])
    ]),
    ],className = 'bg-light p-4 rounded')
],className="p-4",fluid=True)

@callback(
    Output('main-graph', 'figure'),
    Input('product-dropdown', 'value'),
    Input('interval-dropdown', 'value'),
    Input('provider-dropdown', 'value'),
    Input('input-qty', 'value')
)
def update_fig(product,interval,provider,qty):
    fig = create_figure(product=product,interval=interval,filter_groups=provider,qty=qty)
    return fig

@app.callback(
    Output('provider-dropdown', "options"),
    Input('product-dropdown', "value"),
)
def set_provider_options(product):
    return (
        get_provider_groups(product)
    )

if __name__ == '__main__':
    app.run(debug=True)
