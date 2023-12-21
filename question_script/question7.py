import pandas as pd
import plotly.graph_objs as go
import plotly.express as px

from plotly.subplots import make_subplots
from scipy.stats import pearsonr


def get_merged_and_pop_df():
    df = pd.read_pickle('dataset/album_id_and_musics.pickle')
    df1 = pd.read_pickle("dataset/movie_album_and_revenue.pickle")

    df1 = df1[~df1["album_id"].isna()]
    df1.drop_duplicates(subset=['movie_name'], inplace=True)

    empty = df["track"].isna()
    df = df[~empty]
    df.reset_index(inplace=True)
    df["popularity"] = df["track"].apply(lambda x: x.popularity)

    pop_df = df[['album_id', 'popularity']].groupby('album_id').mean()
    pop_df = pop_df[~pop_df["popularity"].isna()]
    pop_df.reset_index(inplace=True)

    merged_df = pd.merge(left=df1, right=pop_df, left_on='album_id', right_on='album_id', how='inner')

    return merged_df, pop_df


def plot_popularity_histogram(pop_df: pd.DataFrame):
    fig = make_subplots()
    fig.add_trace(go.Histogram(x=pop_df['popularity'], nbinsx=5))

    # Update layout with slider
    fig.update_layout(
        sliders=[
            {
                'pad': {"t": 60},
                'currentvalue': {"prefix": "Number of Bins: "},
                'steps': [{'method': 'restyle', 'label': str(i), 'args': [{'nbinsx': i}]} for i in range(5, 16, 5)]
            }
        ]
    )

    # Update axes and layout
    fig.update_xaxes(title_text='Popularity')
    fig.update_yaxes(title_text='Count')
    fig.update_layout(title_text='Interactive Histogram of Popularity')

    # Show the plot
    fig.show()


def plot_scatter_popularity_revenue_by_year(merged_df: pd.DataFrame):
    fig = px.scatter(merged_df, x="popularity", y="movie_revenue", color='release_date', trendline="ols")
    fig.show()


def plot_scatter_popularity_revenue_overall(merged_df: pd.DataFrame):
    fig = px.scatter(merged_df, x="popularity", y="movie_revenue", trendline="ols")
    fig.show()


def print_pearson_correlation(merged_df: pd.DataFrame):
    corr, _ = pearsonr(merged_df["popularity"], merged_df["movie_revenue"])
    print('Pearsons correlation: %.3f' % corr)


def plot_heatmap_correlation(merged_df: pd.DataFrame):
    merged_df['release_date'] = pd.to_datetime(merged_df['release_date'])

    # Extract the year from the 'release_date'
    merged_df['year'] = merged_df['release_date'].dt.year

    # Group by year and calculate the correlation between 'movie_revenue' and 'popularity'
    correlation_by_year = merged_df.groupby('year')[['movie_revenue', 'popularity']].corr().iloc[0::2, -1].reset_index()
    mean_revenue_by_year = merged_df.groupby('year')['movie_revenue'].mean().reset_index()

    correlation_by_year['mean_revenue'] = mean_revenue_by_year['movie_revenue']

    # Rename the columns for the heatmap
    correlation_by_year.columns = ['year', 'drop', 'correlation', 'mean_revenue']
    correlation_by_year = correlation_by_year.drop(columns='drop')
    correlation_by_year.dropna(inplace=True)
    correlation_by_year = correlation_by_year[correlation_by_year["correlation"] < 0.99]
    correlation_by_year = correlation_by_year[correlation_by_year["correlation"] > -0.99]

    display(correlation_by_year)

    # Create the heatmap
    fig = px.imshow(correlation_by_year.pivot(index='year', columns='correlation', values='mean_revenue'),
                    labels=dict(x="Correlation", y="Year", color="Mean Revenue"),
                    y=correlation_by_year['year'],
                    aspect="auto",
                    title="Heatmap of Correlation Between Movie Revenue and Popularity by Year")

    # Update xaxis because there is only one correlation value per year
    fig.update_xaxes(side="top")
    fig.show()
