# Stanislas' music dream : Road to Hollywood !

---

**Authors:** [Luca Carroz](https://people.epfl.ch/emilie.carroz), [David Schroeter](https://people.epfl.ch/david.schroeter), 
[Xavier Ogay](https://people.epfl.ch/xavier.ogay), [Joris Monnet](https://people.epfl.ch/joris.monnet),
[Paulo Ribeiro de Carvalho](https://people.epfl.ch/paulo.ribeirodecarvalho)

**Project Mentor:** [Aoxiang Fan](https://people.epfl.ch/aoxiang.fan) ([Email](mailto:aoxiang.fan@epfl.ch))

---

## Abstract

A 20-year-old aspiring musician, Stanislas, fueled by a passion for the film industry, embarks on a quest to launch his 
career. His ultimate dream? To hear one of his productions featured in a Hollywood film and become one of the planet's 
top composers. To increase his chances, he turns to a team of Data Scientists known as LSD.

The "LearningtheSecretsofData" team's mission is to identify trends shared among successful music composers and compositions, ultimately 
optimizing choices for our young musician. This is not an easy task but the team is driven by the wish of helping 
Stanislas. How could they provoke a cascADA of successful choices in Stany career.

Which music genre Stany should he focus on? Will this new direction be enough for him to conquers the show business? 
Maybe he may invest in a ludicrous website to promote himself? Or should he even consider changing Nationality to 
achieve his goal? Let’s see what’s the plan LSD had concocted for Stanislas.


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

## Dataset Enrichment Method

Missing attributes about movie's composers :

- Name
- Birthday
- Gender
- Homepage
- Place of birth
- First appearance in movie credits

We use a free to use API ([TMDB](https://www.themoviedb.org/?language=fr)) to enrich our movies' information. Also, some 
important features are missing in some observation, that's why we dropped movies not containing the needed information. A specific 
script has been created to be run once and create our `clean_enrich_movie.pickle` dataset. Go to `enrich_movie_data.py` and 
its linked library `tmdb/tmdbDataLoader.py` for more details on how we retrieved these information. 

Missing attributes about composers' musics :

- Genre
- Spotify's popularity

To retrieve these information we used the [SpotifyAPI](https://developer.spotify.com/documentation/web-api). Since 
streams count are impossible to collect, we chose to use the [popularity score](https://developer.spotify.com/documentation/web-api/reference/get-track)
(documentation of score at the end of web page) proposed by the API. Information are stored in `spotify_dataset.pickle`. 
Go to `enrich_music_data.py` and its linked library `spotify/spotify.py` for more details on how we retrieved these information. 

Please note that a personal API key is needed to successfully run the scripts for TMDB ([create key](https://developer.themoviedb.org/reference/intro/getting-started)) 
and Spotify ([create key](https://developer.spotify.com/documentation/web-api/tutorials/getting-started)) dataset creation. 
Make sure to create a file `.env` with your API bearer token using the `.env_example` file as template. 

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

We will use a GitHub page to present our results. The plots will be interactive and will be created using the `plotly`
library. Notably, we want to have a world map with the number of composers per country to answer the question 4.

## Proposed timeline

```
├── 20.11.23 - Work Homework 2
│  
├── 23.11.23 - Work Homework 2
│  
├── 27.11.23 - Work Question 1
│  
├── 30.11.23 - Work Question 2
│  
├── 04.12.23 - Homework 2 deadline
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

## Organization within the Team

| Xavier       | Paulo   | David        | Luca    | Joris        |
|--------------|---------|--------------|---------|--------------|
| Q.7, Website | Q.1 & 4 | Q.2, Website | Q.6 & 5 | Q.3, Website |

## Questions for TAs

None
