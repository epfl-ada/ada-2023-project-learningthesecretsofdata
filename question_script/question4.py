"""
Answer fourth question of project:
Where do composers come from ?
"""
import pandas as pd
import plotly.express as px


def composers_selection_over_popularity(df: pd.DataFrame, min_popularity: int, max_popularity: int) -> pd.DataFrame:
    """Description"""
    movie_in_revenue_range = df.popularity.apply(lambda p: True if min_popularity <= p <= max_popularity else False)
    return df[movie_in_revenue_range]


def heat_map_world(df: pd.DataFrame, color: str):
    # Create an interactive world map heatmap using Plotly Express
    fig = px.choropleth(data_frame=df.reset_index(),
                        locations='country',
                        locationmode='country names',
                        color='location',
                        color_continuous_scale=color,  # can change color, click link in markdown just above
                        title='Heat Map of Locations',
                        labels={'location': 'Number of Composers'}
                        )
    # Fix the layout template
    fig.update_layout(template='plotly')

    # Show the figure
    fig.show()

