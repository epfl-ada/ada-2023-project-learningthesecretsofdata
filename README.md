<!-- Able a quick return to the top page -->
<a name="readme-top"></a>

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/epfl-ada/ada-2023-project-learningthesecretsofdata">
    <img src="assets/img/LSD_trans.png" alt="Logo" width="80" height="80">
  </a>

<h3 align="center">Stanislas' Music Dream : Road to Hollywood !</h3>

  <p align="center">
    An awesome journey into Applied Data Analysis through movie dataset
    <br />
    <a href="https://learningthesecretsofdata.github.io/CS-401_Website/"><strong>Explore the Website »</strong></a>
  </p>
</div>

---

**Authors:** [Luca Carroz](https://people.epfl.ch/emilie.carroz),
[David Schroeter](https://people.epfl.ch/david.schroeter),
[Xavier Ogay](https://people.epfl.ch/xavier.ogay), [Joris Monnet](https://people.epfl.ch/joris.monnet),
[Paulo Ribeiro de Carvalho](https://people.epfl.ch/paulo.ribeirodecarvalho)

**Project Mentor:** [Aoxiang Fan](https://people.epfl.ch/aoxiang.fan) ([Email](mailto:aoxiang.fan@epfl.ch))

---


<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#abstract">Abstract</a></li>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li><a href="#research-questions">Research Questions</a></li>
    <li><a href="#dataset-enrichment-method">Dataset Enrichment Method</a></li>
    <li>
      <a href="#methods">Methods</a>
      <ul>
        <li><a href="#data-loading">Data Loading</a></li>
        <li><a href="#data-cleaning">Data Cleaning</a></li>
        <li><a href="#data-visualization">Data Visualization</a></li>
        <li><a href="#data-processing">Data Processing</a></li>
      </ul>
    </li>
    <li><a href="#project-timeline">Project Timeline</a></li>
    <li><a href="#organization-within-the-team">Organization Within the Team</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->

## About The Project

[![Road-to-holywood][product-screenshot]](https://learningthesecretsofdata.github.io/CS-401_Website/)

### Abstract

A 20-year-old aspiring musician, Stanislas, fueled by a passion for the film industry, embarks on a quest to launch his
career. His ultimate dream? To hear one of his productions featured in a Hollywood film and become one of the planet's
top composers. To increase his chances, he turns to a team of Data Scientists known as LSD.

The "LearningtheSecretsofData" team's mission is to identify trends shared among successful music composers and
compositions, ultimately
optimizing choices for our young musician. This is not an easy task but the team is driven by the wish of helping
Stanislas. How could they provoke a cascADA of successful choices in Stany career.

Which music genre Stany should he focus on? Will this new direction be enough for him to conquers the show business?
Maybe he may invest in a ludicrous website to promote himself? Or should he even consider changing Nationality to
achieve his goal? Let’s see what’s the plan LSD had concocted for Stanislas.

### Built With

* [![Python][Python.org]][Python-url]
* [![Plotly][Plotly.com]][Plotly-url]
* [![BeautifulJekyll][BeautifulJekyll.com]][BeautifulJekyll-url]

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Research Questions

1) Which are the most frequent music genre appearing in movies ?
2) What is the average composer's age at their :
    - first movie appearance ?
    - biggest box office revenue ?
3) How the top composers' career progress over the years ?
4) Where do composers come from ?
5) Does composer's gender matter ?
6) Does having a personal website correlate with the composers' success ?
7) Is there a correlation between box office revenue and movie's playlist popularity ?

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Dataset Enrichment Method

Missing attributes about movie's composers :

- Name
- Birthday
- Gender
- Homepage
- Place of birth
- First appearance in movie credits

We use the free to use [TMDB API](https://www.themoviedb.org/?language=fr) to enrich our movies' information. Also, some
important features are missing in some observation, that's why we dropped movies not containing the needed information.
A specific
script has been created to be run once and create our `clean_enrich_movie.pickle` dataset. Go to `enrich_movie_data.py`
and
its linked library `tmdb/tmdbDataLoader.py` for more details on how we retrieved these information.

Missing attributes about composers' musics :

- Genre
- Spotify's popularity

To retrieve these information we used the [SpotifyAPI](https://developer.spotify.com/documentation/web-api). Since
streams count are impossible to collect, we chose to use
the [popularity score](https://developer.spotify.com/documentation/web-api/reference/get-track)
(documentation of score at the end of web page) proposed by the API. Information are stored in `spotify_dataset.pickle`
and `album_id_and_musics.pickle`.
Go to `enrich_music_data.py`, `enrich_with_spotify_data.py` and its linked library `spotify/spotify.py` for more details
on how we retrieved these information.

We had to proceed to a mapping between the movie's name and the album's name so that we could retrieve the popularity
score of each tracks of the album.
To do so, we used the library [rapidfuzz](https://pypi.org/project/rapidfuzz/) which use the calculation
of [FuzzyWuzzy](https://github.com/seatgeek/fuzzywuzzy) to match the movie's name and the album's name.
We also leverage the Fuzz ratio with some negative and positive keyword to improve the matching of soundtrack albums.
The matching is saved in `movie_album_and_revenue.pickle`. Then the script `enrich_with_spotify_data.py` use the
previously generated matching file to retrieve all tracks of the albums. This new dataset is saved to the
file `movie_album_and_revenue_with_track_ids.pickle`.
The popularity score of each album is then computed as the mean of popularity score of each tracks of the corresponding
album.
The script `enrich_with_spotify_data.py` make each API call to retrieve data asynchroneously and in batch to speed up
the process. It also save checkpoint of the data retrieved to avoid losing data in case of network error.

Please note that a personal API key is needed to successfully run the scripts for
TMDB ([create key](https://developer.themoviedb.org/reference/intro/getting-started))
and Spotify ([create key](https://developer.spotify.com/documentation/web-api/tutorials/getting-started)) dataset
creation.
Make sure to create a file `.env` with your API bearer token using the `.env_example` file as template.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Methods

### Data Loading

We load the data from the `clean_enrich_movie.pickle` file. This file contains all the information about the movies and
the composers. We also load the `spotify_dataset.pickle` file which contains the information about the music genre and
the popularity of the music. We use the first dataset to answer the questions 2, 3, 4, 5 and 6. We use the second
dataset to answer the question 1 & 7.

### Data Cleaning

We clean the data by removing the entry with missing value in their features 'name', 'release_date',
'countries', 'genres'. For missing 'box_office_revenue', we call TMDB API to try to retrieve the information.
If the API call fails to return a value for the revenue, we remove the entry.
We also format the release date to integer and sort the data by revenue.

### Data Visualization

We use a GitHub page to present our results. The plots are interactive and were created using the `plotly`
library. Notably, we used a world map with the number of composers per country to answer the question 4.
We also used pie charts, bar charts, line plots, and more. Everything is interactive!

### Data Processing

We utilize the [OpenAI API](https://platform.openai.com/docs/introduction) to assist us in processing data.
Specifically, in order to address one of our research questions, we require a method to convert locations into
countries. For instance, the input "New York City, New York, United States of America" should be associated with the
output "United States." Additionally, various location variations should be standardized to the same country name; for
example, both "USA" and "United States" should be mapped to "United States." To achieve this mapping, we provide the
GPT-4 model with our dataset through an API request via `location_to_country_openai_api.py`, asking it to provide a 
mapping dictionary. The resulting dictionary is then transformed into a new dataframe and saved in our repository as 
`mapping_locations_to_country.csv`.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Project Timeline

```
├── 27.11.23 - Work Question 1
│  
├── 30.11.23 - Work Question 2
│    
├── 04.12.23 - Work Question 3
│  
├── 07.12.23 - Work Questions 4 & 5
|
├── 11.12.23 - Work Question 6 & 7
│
├── 14.12.23 - Work on visualization/website
│  
├── 18.12.23 - Work on visualization/website
│    
├── 22.12.23 - Milestone 3 deadline
│  
├── 25.12.23 - Merry Christmas!
```

## Contributions of the Team Members

| Xavier      | Paulo            | David    | Luca    | Joris       |
|-------------|------------------|----------|---------|-------------|
| Q.7         | Q.1              | Q.2      | Q.6     | Q.3         |
| API Spotify | Q.4 / OpenAI API | API TMDB | Q.5     | API Spotify |
| Website     | Website          | Website  | Website | Website     |

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->

[Python.org]: https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white

[Python-url]: https://www.python.org/

[Plotly.com]: https://img.shields.io/badge/Plotly-239120?style=for-the-badge&logo=plotly&logoColor=white

[Plotly-url]: https://plotly.com/

[BeautifulJekyll.com]: https://img.shields.io/badge/Beautiful%20Jekyll-%23FF0000.svg?style=for-the-badge&logo=Jekyll&logoColor=white

[BeautifulJekyll-url]: https://beautifuljekyll.com/

[product-screenshot]: assets/img/header_holy.png









