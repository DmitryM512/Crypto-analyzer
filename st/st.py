import streamlit as st
import plotly.express as px
import pandas as pd
import psycopg2
from binance.client import Client
from settings import settings
import time


def load_figures(pair: str) -> pd.DataFrame:
    conn = psycopg2.connect(dbname="main", user="analyzer", password="analyzer", host="pg")
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM bid_ask_ratio WHERE pair = '{pair}' ORDER BY id DESC ")
    data = cursor.fetchall()
    cursor.close()
    conn.close()

    df = pd.DataFrame(data,
                      columns=['id', 'time', 'pair', 'bid_ask_ratio_total', 'bid_ask_ratio_50', 'bid_ask_ratio_20',
                               'bid_ask_ratio_8', 'bid_ask_ratio_5', 'bid_ask_ratio_3',
                               'limit_density', 'total_orders', 'sell_buy_ratio', 'buy_sell_ratio', 'delta',
                               'bid_density_total', 'ask_density_total', 'bid_density_50', 'ask_density_50',
                               'bid_density_20', 'ask_density_20', 'bid_density_8', 'ask_density_8', 'bid_density_5',
                               'ask_density_5', 'bid_density_3', 'ask_density_3'])
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values(by='time')
    df['Normalized delta'] = df['delta'] / df['delta'].abs().max()
    df['Delta'] = df['Normalized delta'].rolling(60).mean()

    if pair in ['BTCUSDT', 'ETHUSDT']:
        df['100%'] = df['bid_ask_ratio_total'].rolling(60).mean()
        df['50%'] = df['bid_ask_ratio_50'].rolling(60).mean()
        df['20%'] = df['bid_ask_ratio_20'].rolling(60).mean()
    else:
        df['8%'] = df['bid_ask_ratio_8'].rolling(60).mean()
        df['5%'] = df['bid_ask_ratio_5'].rolling(60).mean()
        df['20%'] = df['bid_ask_ratio_20'].rolling(60).mean()

    return df


def fetch_book(pair: str) -> object:
    client = Client(api_key=settings.binance_api_key, api_secret=settings.binance_api_secret)
    data = client.get_order_book(symbol=pair, limit=5000)
    bids = data['bids']
    asks = data['asks']
    df_bids = pd.DataFrame(bids)
    df_asks = pd.DataFrame(asks)
    df_bids = df_bids.astype(float)
    df_bids.columns = ['price', 'amount']
    df_bids['type'] = 'Bid'
    df_asks = df_asks.astype(float)
    df_asks.columns = ['price', 'amount']
    df_asks['type'] = 'Ask'
    new_df = pd.concat([df_bids, df_asks])

    return px.histogram(new_df, x='amount', y='price', orientation='h', nbins=40, color='type', barmode="overlay",
                        color_discrete_sequence=["green", "red"])


def fetch_agg_trades(pair: str) -> object:
    conn = psycopg2.connect(dbname="main", user="analyzer", password="analyzer", host="pg")
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM agg_trade WHERE pair = '{pair}' ")
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(data, columns=['id', 'time', 'pair', 'buy', 'sell', 'buy_recent_qty', 'sell_recent_qty',
                                     'buy_recent_count', 'sell_recent_count'])

    df['time'] = pd.to_datetime(df['time'])
    df['time'] = df['time'].dt.floor(freq='min')
    result = df.groupby('time').sum()[['buy', 'sell', 'buy_recent_qty', 'sell_recent_qty', 'buy_recent_count',
                                       'sell_recent_count']]
    result = result.reset_index()
    result['Buy_flow'] = result['buy'].rolling(60).mean()
    result['Sell_flow'] = result['sell'].rolling(60).mean()
    result['Buy_flow'] = (result['Buy_flow'] - result['Buy_flow'].min()) /\
                         (result['Buy_flow'].max() - result['Buy_flow'].min())
    result['Sell_flow'] = (result['Sell_flow'] - result['Sell_flow'].min()) /\
                         (result['Sell_flow'].max() - result['Sell_flow'].min())
    result['Count'] = (result['buy_recent_count'] + result['sell_recent_count']).rolling(60).mean()
    result['Buy_qty'] = result['buy_recent_qty'].rolling(60).mean()
    result['Sell_qty'] = result['sell_recent_qty'].rolling(60).mean()

    return result


st.set_page_config(
    page_title="Analyzer",
    page_icon="🧊",
    layout="wide")

st.title('Analyzer')
instrument = st.selectbox('Select the instrument', ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'SHIBUSDT',
        'ADAUSDT'))

col1, col2 = st.columns(2)
main_df = load_figures(instrument)

dict_settings = dict(
        rangeselector=dict(
            buttons=list([
                dict(count=1,
                     label="1m",
                     step="month",
                     stepmode="backward"),
                dict(count=10,
                     label="10d",
                     step="day",
                     stepmode="backward"),
                dict(count=5,
                     label="5d",
                     step="day",
                     stepmode="backward")
             ])
        ),
        rangeslider=dict(visible=True),
        range=[main_df['time'].to_list()[-3500], main_df['time'].to_list()[-1]],
        type="date")

with col1:
    if instrument in ['BTCUSDT', 'ETHUSDT']:
        bid_ask_ratio = px.line(main_df, x='time', y=['100%', '50%', '20%'])
    else:
        bid_ask_ratio = px.line(main_df, x='time', y=['20%', '8%', '5%'])
    bid_ask_ratio.update_layout(xaxis=dict_settings)

    st.subheader('Bid ask ratio')
    st.plotly_chart(bid_ask_ratio)
with col2:
    st.subheader('Order book')
    book = fetch_book(instrument)
    st.plotly_chart(book)

col3, col4 = st.columns(2)
agg_df = fetch_agg_trades(instrument)
with col3:
    limit_density = px.line(main_df, x='time', y=['Delta'])
    limit_density.update_layout(xaxis=dict_settings)
    st.subheader('Normalized Delta')
    st.plotly_chart(limit_density)

with col4:
    agg_trade_graph = px.line(agg_df, x='time', y=['Buy_flow', 'Sell_flow'], color_discrete_sequence=["green", "red"])
    agg_trade_graph.update_layout(xaxis=dict_settings)
    st.subheader('Aggregated trades')
    st.plotly_chart(agg_trade_graph)

col5, col6 = st.columns(2)
with col5:
    trades_quant = px.line(agg_df, x='time', y=['Count'])
    trades_quant.update_layout(xaxis=dict_settings)
    st.subheader('Quantity trades')
    st.plotly_chart(trades_quant)

with col6:
    trades_qty = px.line(agg_df, x='time', y=['Buy_qty', 'Sell_qty'], color_discrete_sequence=["green", "red"])
    trades_qty.update_layout(xaxis=dict_settings)
    st.subheader('Qty recent trades')
    st.plotly_chart(trades_qty)

time.sleep(300)
st.rerun()
