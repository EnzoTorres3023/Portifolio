# 1. Importando as bibliotecas necessárias
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html
import statsmodels

# --- PREPARAÇÃO DOS DADOS ---
# 2. Carregando e limpando o dataframe
df = pd.read_csv('ecommerce_estatistica.csv')
df.dropna(inplace=True)

# --- CRIAÇÃO DAS FIGURAS (GRÁFICOS) ---

# Gráfico 1: Histograma de Preços
fig_hist = px.histogram(df, x='Preço', nbins=50, title='Distribuição de Preços dos Produtos',
                        labels={'Preço': 'Preço (R$)', 'count': 'Quantidade de Produtos'})
fig_hist.update_layout(bargap=0.1)

# Gráfico 2: Dispersão (Scatter Plot) - Preço vs. Nota
fig_scatter = px.scatter(df, x='Preço', y='Nota', title='Relação entre Preço e Nota',
                         labels={'Preço': 'Preço (R$)', 'Nota': 'Nota Média'})

# Gráfico 3: Mapa de Calor (Heatmap) de Correlações
cols_corr = ['Nota', 'N_Avaliações', 'Desconto', 'Preço', 'Qtd_Vendidos_Cod']
corr_matrix = df[cols_corr].corr()
fig_heatmap = go.Figure(data=go.Heatmap(
                   z=corr_matrix.values,
                   x=corr_matrix.columns,
                   y=corr_matrix.columns,
                   colorscale='RdBu_r',
                   zmin=-1, zmax=1))
fig_heatmap.update_layout(title='Mapa de Calor de Correlações')


# Gráfico 4: Gráfico de Barras - Top 10 Marcas por Nota
marca_por_nota = df.groupby('Marca')['Nota'].mean().sort_values(ascending=False).head(10)
fig_bar = px.bar(marca_por_nota, x='Nota', y=marca_por_nota.index,
                 title='Top 10 Marcas por Nota Média', orientation='h',
                 labels={'Nota': 'Nota Média', 'y': 'Marca'})
fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})


# Gráfico 5: Gráfico de Pizza - Proporção por Temporada
temporada_counts = df['Temporada'].value_counts()
fig_pie = px.pie(values=temporada_counts.values, names=temporada_counts.index,
                 title='Proporção de Produtos por Temporada', hole=.3)

# Gráfico 6: Gráfico de Regressão - Vendas vs. Avaliações
df_sample = df.sample(n=1000, random_state=42) if len(df) > 1000 else df
fig_reg = px.scatter(df_sample, x='Qtd_Vendidos_Cod', y='N_Avaliações',
                     trendline="ols",
                     title='Tendência: Vendas vs. Número de Avaliações',
                     labels={'Qtd_Vendidos_Cod': 'Quantidade Vendida (Codificada)', 'N_Avaliações': 'Número de Avaliações'})


# --- CONSTRUÇÃO DA APLICAÇÃO DASH ---
app = Dash(__name__)
server = app.server

app.layout = html.Div(children=[
    html.H1(children='Dashboard de Análise de E-commerce', style={'textAlign': 'center', 'color': '#1f77b4'}),
    html.Div(children='Uma visão interativa sobre os dados de produtos, vendas e avaliações.', style={'textAlign': 'center', 'marginBottom': '30px'}),
    html.Div(className='row', children=[
        html.Div(dcc.Graph(id='histograma-precos', figure=fig_hist), className='six columns'),
        html.Div(dcc.Graph(id='scatter-preco-nota', figure=fig_scatter), className='six columns'),
    ], style={'display': 'flex'}),
    html.Div(className='row', children=[
        html.Div(dcc.Graph(id='heatmap-correlacoes', figure=fig_heatmap), className='six columns'),
        html.Div(dcc.Graph(id='barras-marcas', figure=fig_bar), className='six columns'),
    ], style={'display': 'flex', 'marginTop': '20px'}),
    html.Div(className='row', children=[
        html.Div(dcc.Graph(id='pizza-temporada', figure=fig_pie), className='six columns'),
        html.Div(dcc.Graph(id='regressao-vendas', figure=fig_reg), className='six columns'),
    ], style={'display': 'flex', 'marginTop': '20px'}),
], style={'fontFamily': 'Arial, sans-serif'})


# 5. Rodando o servidor
if __name__ == '__main__':
    app.run(debug=True)