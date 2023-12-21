import pandas as pd
import plotly.graph_objs as go
import plotly.express as px
import seaborn as sns

from matplotlib import pyplot as plt
from plotly.subplots import make_subplots
from scipy.stats import pearsonr


def get_merged_and_pop_df():
    """
    Get the merged dataframe and the popularity dataframe for Q7
    """

    # Read the pickle files
    df = pd.read_pickle('dataset/album_id_and_musics.pickle')
    df1 = pd.read_pickle("dataset/movie_album_and_revenue.pickle")

    # Drop the rows with missing values
    df1 = df1[~df1["album_id"].isna()]
    df1.drop_duplicates(subset=['movie_name'], inplace=True)

    # Drop the rows with missing values
    empty = df["track"].isna()
    df = df[~empty]

    df.reset_index(inplace=True)
    df["popularity"] = df["track"].apply(lambda x: x.popularity)

    # Get the mean popularity for each album
    pop_df = df[['album_id', 'popularity']].groupby('album_id').mean()
    pop_df = pop_df[~pop_df["popularity"].isna()]
    pop_df.reset_index(inplace=True)

    merged_df = pd.merge(left=df1, right=pop_df, left_on='album_id', right_on='album_id', how='inner')

    return merged_df, pop_df


def plot_popularity_histogram(pop_df: pd.DataFrame):
    """
    Plot the histogram of the popularity

    Parameters
    ----------
    pop_df: pd.DataFrame
        The dataframe containing the popularity information
    """
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


def plot_popularity_histogram_matplt(pop_df: pd.DataFrame):
    """
    Plot the histogram of the popularity using Matplotlib.

    Parameters
    ----------
    pop_df: pd.DataFrame
        The dataframe containing the popularity information.
    """

    # Set the number of bins
    bins = 50

    # Create the histogram
    plt.hist(pop_df['popularity'], bins=bins, color='blue', edgecolor='black')

    # Set the title and labels
    plt.title('Histogram of Popularity')
    plt.xlabel('Popularity')
    plt.ylabel('Count')

    # Show the plot
    plt.show()


def plot_scatter_popularity_revenue_by_year(merged_df: pd.DataFrame):
    """
    Plot the scatter plot of popularity and revenue by year

    Parameters
    ----------
    merged_df: pd.DataFrame
        The dataframe containing the popularity and revenue information
    """
    fig = px.scatter(merged_df, x="popularity", y="movie_revenue", color='release_date', trendline="ols")
    fig.show()


def plot_scatter_popularity_revenue_by_year_matplotlib(merged_df: pd.DataFrame):
    """
    Plot the scatter plot of popularity and revenue by year with Matplotlib

    Parameters
    ----------
    merged_df: pd.DataFrame
        The dataframe containing the popularity and revenue information
    """

    merged_df.dropna(inplace=True)
    # drop row with 0 value
    merged_df = merged_df[merged_df["movie_revenue"] > 0]
    merged_df = merged_df[merged_df["popularity"] > 0]

    # popularity and revenue cast as int
    merged_df["popularity"] = merged_df["popularity"].astype(int)
    merged_df["movie_revenue"] = merged_df["movie_revenue"].astype(int)

    # Create a scatter plot using seaborn for better color handling and regression line
    sns.lmplot(x='popularity', y='movie_revenue', hue='release_date', data=merged_df,
               aspect=1.5, fit_reg=True, scatter_kws={'alpha': 0.8},legend=False)

    # Set title and labels
    plt.title('Scatter Plot of Popularity vs Movie Revenue by Year')
    plt.xlabel('Popularity')
    plt.ylabel('Movie Revenue')

    # Show the plot
    plt.show()


def plot_scatter_popularity_revenue_overall(merged_df: pd.DataFrame):
    """
    Plot the scatter plot of popularity and revenue overall

    Parameters
    ----------
    merged_df: pd.DataFrame
        The dataframe containing the popularity and revenue information
    """
    fig = px.scatter(merged_df, x="popularity", y="movie_revenue", trendline="ols")
    fig.show()


def plot_scatter_popularity_revenue_overall_matplotlib(merged_df: pd.DataFrame):
    """
    Plot the scatter plot of popularity and revenue overall with Matplotlib

    Parameters
    ----------
    merged_df: pd.DataFrame
        The dataframe containing the popularity and revenue information
    """
    # Create a scatter plot with a regression line
    sns.lmplot(x='popularity', y='movie_revenue', data=merged_df, fit_reg=True, scatter_kws={'alpha': 0.8},
               line_kws={'color': 'red'})

    # Set title and labels
    plt.title('Scatter Plot of Popularity vs Movie Revenue')
    plt.xlabel('Popularity')
    plt.ylabel('Movie Revenue')

    # Show the plot
    plt.show()


def print_pearson_correlation(merged_df: pd.DataFrame):
    """
    Print the pearson correlation between popularity and revenue

    Parameters
    ----------
    merged_df: pd.DataFrame
        The dataframe containing the popularity and revenue information
    """
    corr, _ = pearsonr(merged_df["popularity"], merged_df["movie_revenue"])
    print('Pearsons correlation: %.3f' % corr)


def plot_heatmap_correlation(merged_df: pd.DataFrame):
    """
    Plot the heatmap of correlation between popularity and revenue

    Parameters
    ----------
    merged_df: pd.DataFrame
        The dataframe containing the popularity and revenue information
    """
    merged_df_modified = merged_df.copy()
    merged_df_modified['release_date'] = pd.to_datetime(merged_df_modified['release_date'])

    # Extract the year from the 'release_date'
    merged_df_modified['year'] = merged_df_modified['release_date'].dt.year

    # Group by year and calculate the correlation between 'movie_revenue' and 'popularity'
    correlation_by_year = merged_df_modified.groupby('year')[['movie_revenue', 'popularity']].corr().iloc[0::2, -1].reset_index()
    mean_revenue_by_year = merged_df_modified.groupby('year')['movie_revenue'].mean().reset_index()

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


def plot_heatmap_correlation_matplotlib(merged_df: pd.DataFrame):
    """
    Plot the heatmap of correlation between popularity and revenue with Matplotlib

    Parameters
    ----------
    merged_df: pd.DataFrame
        The dataframe containing the popularity and revenue information
    """

    merged_df_modified = merged_df.copy()
    merged_df_modified.dropna(inplace=True)
    # drop row with 0 value
    merged_df_modified = merged_df_modified[merged_df_modified["movie_revenue"] > 0]
    merged_df_modified = merged_df_modified[merged_df_modified["popularity"] > 0]
    merged_df_modified['release_date'] = merged_df_modified['release_date'].astype(int)
    merged_df_modified = merged_df_modified[merged_df_modified["release_date"] > 1000]

    # Group by year and calculate the correlation between 'movie_revenue' and 'popularity'
    correlation_by_year = merged_df_modified.groupby('release_date')[['movie_revenue', 'popularity']].corr().iloc[0::2, -1].reset_index()
    mean_revenue_by_year = merged_df_modified.groupby('release_date')['movie_revenue'].mean().reset_index()

    correlation_by_year['mean_revenue'] = mean_revenue_by_year['movie_revenue']

    # Rename the columns for the heatmap
    correlation_by_year.columns = ['year', 'drop', 'correlation', 'mean_revenue']
    correlation_by_year = correlation_by_year.drop(columns='drop')
    correlation_by_year.dropna(inplace=True)
    correlation_by_year = correlation_by_year[correlation_by_year["correlation"] < 0.99]
    correlation_by_year = correlation_by_year[correlation_by_year["correlation"] > -0.99]

    correlation_by_year['year'] = correlation_by_year['year'].astype(int)
    correlation_by_year['correlation'] = correlation_by_year['correlation'].astype(float)
    correlation_by_year['mean_revenue'] = correlation_by_year['mean_revenue'].astype(float)

    #round correlation
    correlation_by_year['correlation'] = correlation_by_year['correlation'].apply(lambda x: round(x, 3))

    # Create the heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_by_year.pivot(index='year', columns='correlation', values='mean_revenue'), annot=True, fmt=".2f", cmap='viridis')

    # Set the title and labels
    plt.title('Heatmap of Correlation Between Movie Revenue and Popularity by Year')
    plt.xlabel('Correlation')
    plt.ylabel('Year')

    # Show the plot
    plt.show()
