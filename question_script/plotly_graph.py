import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from plotly.subplots import make_subplots


def create_plotly_number_of_movies(movie_grouped_by_top_composer):
    """
    Save the plotly figure for the number of movies per composer
    :param movie_grouped_by_top_composer: The dataframe grouped by composer
    :return: None
    """
    new_df = movie_grouped_by_top_composer.copy()

    new_df.dropna(inplace=True)

    # Create a column count which contains the number of movies per year and per composer
    # Group by composer and year bin, count the number of movies
    movie_counts = movie_grouped_by_top_composer.groupby(['composer_name', 'year_bin'], observed=False).size()

    # Unstack the 'composer_name' level to create a DataFrame
    movie_counts_df = movie_counts.unstack(level='composer_name')

    # Create a df with the year bins and composer_name as columns
    movie_counts_df = movie_counts_df.reset_index()

    # Rename the columns
    movie_counts_df.columns = ['year_bin'] + list(movie_counts_df.columns[1:])
    movie_counts_df = movie_counts_df.sort_values(by='year_bin')

    # Replace bins like (1900, 1905] by 1900 - 1905
    movie_counts_df['year_bin'] = movie_counts_df['year_bin'].apply(
        lambda x: str(x).replace('(', '')
        .replace(']', '')
        .replace(',', ' -')
    )

    fig = px.line(movie_counts_df, x='year_bin', y=list(movie_counts_df.columns[1:]),
                  title='Number of movies per composer')

    # Get the list of unique composers
    composers = new_df['composer_name'].unique()

    # Add a dropdown menu to select the composers to display
    dropdown = []
    for i, composer in enumerate(composers):
        visible = [False] * len(composers)
        visible[i] = True
        dropdown.append(dict(
            method='update',
            label=composer,
            args=[{'visible': visible},
                  {'title': composer}]))
    all_button = dict(
        method='update',
        label='All',
        args=[{'visible': new_df['composer_name'].isin(new_df['composer_name'].unique())},
              {'title': 'All'}])

    # Prepend the "All" button to the dropdown list
    dropdown.insert(0, all_button)

    dropdown.sort(key=lambda x: x['label'])

    # Add Dropdown menu and select All by default
    fig.update_layout(
        updatemenus=[
            dict(
                buttons=dropdown,
                direction="down",
                pad={"r": 10, "t": 10},
                showactive=True,
                x=1.0,
                xanchor="left",
                y=1.32,
                yanchor="top",
                font=dict(color='#000000')
            ),
        ],
        xaxis_title="Year",
        yaxis_title="Number of Movies",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        legend_title_text='Composers',
    )
    fig.update_traces(mode='lines')

    fig.write_html("Q3_number_of_movies_per_year.html")


def create_plotly_box_office_revenue(movie_grouped_by_top_composer):
    """
    Save the plotly figure for the box office revenue per composer
    :param movie_grouped_by_top_composer: The dataframe grouped by composer
    :return: None
    """
    new_df = movie_grouped_by_top_composer.copy()

    new_df.dropna(inplace=True)
    new_df['year_bin'] = new_df['year_bin'].astype(str)

    # Sum the box office revenue per year and per composer
    new_df = new_df.groupby(['composer_name', 'year_bin'], observed=False)['box_office_revenue'].sum().reset_index()

    new_df = new_df.sort_values(by='year_bin')

    # Replace bins like (1900, 1905] by 1900 - 1905
    new_df['year_bin'] = new_df['year_bin'].apply(
        lambda x: str(x).replace('(', '')
        .replace(']', '')
        .replace(',', ' -')
    )

    fig = px.line(new_df, x='year_bin', y='box_office_revenue', color='composer_name',
                  title='Sum of the Box-Office Revenues per composer')

    # Get the list of unique composers
    composers = new_df['composer_name'].unique()

    # Add a dropdown menu to select the composers to display
    dropdown = []
    for i, composer in enumerate(composers):
        visible = [False] * len(composers)
        visible[i] = True
        dropdown.append(dict(
            method='update',
            label=composer,
            args=[{'visible': visible},
                  {'title': composer}]))

    all_button = dict(
        method='update',
        label='All',
        args=[{'visible': new_df['composer_name'].isin(new_df['composer_name'].unique())},
              {'title': 'All'}])

    # Prepend the "All" button to the dropdown list
    dropdown.insert(0, all_button)

    dropdown.sort(key=lambda x: x['label'])

    # Add Dropdown menu and select All by default
    fig.update_layout(
        updatemenus=[
            dict(
                buttons=dropdown,
                direction="down",
                pad={"r": 10, "t": 10},
                showactive=True,
                x=1.0,
                xanchor="left",
                y=1.32,
                yanchor="top",
                font=dict(color='#000000')
            ),
        ],
        xaxis_title="Year",
        yaxis_title="Sum of the Box-Office Revenues",
        legend_title_text='Composers',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )

    fig.update_traces(mode='lines')

    fig.write_html("Q3_box_office_revenue_per_year.html")


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
    correlation_by_year = merged_df_modified.groupby('year')[['movie_revenue', 'popularity']].corr().iloc[0::2,
                          -1].reset_index()
    mean_revenue_by_year = merged_df_modified.groupby('year')['movie_revenue'].mean().reset_index()

    correlation_by_year['mean_revenue'] = mean_revenue_by_year['movie_revenue']

    # Rename the columns for the heatmap
    correlation_by_year.columns = ['year', 'drop', 'correlation', 'mean_revenue']
    correlation_by_year = correlation_by_year.drop(columns='drop')
    correlation_by_year.dropna(inplace=True)
    correlation_by_year = correlation_by_year[correlation_by_year["correlation"] < 0.99]
    correlation_by_year = correlation_by_year[correlation_by_year["correlation"] > -0.99]

    display(correlation_by_year)

    # Create the heatmap using Graph Objects
    fig = go.Figure(data=go.Scatter(
        x=correlation_by_year['correlation'],
        y=correlation_by_year['year'],
        mode='markers',
        marker=dict(
            size=10,  # you can adjust the size of the points here
            color=correlation_by_year['mean_revenue'],  # Color of points based on mean revenue
            colorbar=dict(title='Mean Year Revenue'),
            showscale=True
        ),
        hoverinfo='text',
        text=correlation_by_year['mean_revenue']  # Text to show on hover, here it's mean revenue
    ))

    # Update layout
    fig.update_layout(
        title='',
        xaxis=dict(title='Correlation between Movie revenues and Popularity by Year', side='top'),
        yaxis=dict(title='Year'),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        showlegend=False
    )

    fig.update_xaxes(side="top")
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        hovermode='y'
    )
    fig.update_yaxes(
        showspikes=True,
        spikemode='across',
        spikesnap='data',
        spikecolor="rgba(183,73,87,30)",
        spikethickness=-2
    )
    fig.update_xaxes(
        showspikes=True,
        spikemode='across',
        spikesnap='data',
        spikecolor="rgba(183,73,87,30)",
        spikethickness=-2
    )
    fig.update_layout(
        spikedistance=-1,
    )

    fig.update_traces(
        hovertemplate="<br>".join([
            "Correlation: %{x}",
            "Year: %{y}"
        ])
    )

    fig.show()
    fig.write_html("Q7_correlation_heatmap.html")
