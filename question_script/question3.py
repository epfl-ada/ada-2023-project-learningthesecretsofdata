import plotly.express as px


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
