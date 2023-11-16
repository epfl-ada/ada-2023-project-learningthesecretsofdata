# Stanislas' dream needs help !

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

The LSD team's mission is to identify trends shared among successful music composers and compositions, ultimately 
optimizing choices for our young musician. The storyline focuses on American movies and Successful composers (successful
meaning those who have produced soundtracks for high-grossing films, music appearing in more than ?? films, etc.)

## Research Questions

1) Which are the most frequent music genre appearing in movies ?
2) What is the average composer's age at their first movie appearance ?
3) Where do composers come from ?
4) Does composer's gender matter ?
5) Does personal website is preferred ?
6) What is the expected **fame** to appear in a movie ? (Challenging : **fame** can be quantified with the amount of 
7) streams or CD selling for older compositors.)

## Additional datasets

List of needed information about movie's composer :
- Name
- Birthday
- Gender
- Homepage
- Place of birth
- First appearance in movie

We use a free to use API ([TMDB](https://www.themoviedb.org/?language=fr)) to enrich our movies' information. Also, some 
important features are missing in some observation, we then drop movies not containing the needed information. A specific 
script has been created to be run once and create our `clean_enrich_movie.pickle` dataset. Go to `enrich_data.py` and 
its linked library `tmdb/tmdb.py` for more explanation on how we retrieved these information. Please note that a personal 
API key is needed to successfully run the script ([create key](https://developer.themoviedb.org/reference/intro/getting-started)). 
Make sure to create file `.env` with your API bearer token using the `.env_example` as template.

## Methods

Talk about the API. What is the technology ? Why did we use it instead of already existing dataset ? 

## Proposed timeline

```
├── 20.11.23 - 
│  
├── 23.11.23 - 
│  
├── 27.11.23 -
│  
├── 30.11.23 -
│  
├── 04.12.23 - Homework 2 deadline
│  
├── 04.12.23 -
│  
├── 07.12.23 -
|
├── 11.12.23 - 
│
├── 14.12.23 - 
│  
├── 18.12.23 - 
│    
├── 22.12.23 - Milestone 3 deadline
│  
├── 25.12.23 - Merry Christmas!

```

## Organization within the Team

just a random example.

| Date  | Xavier | Paulo            | David            | Luca              | Joris             |
|-------|--------|------------------|------------------|-------------------|-------------------|
| 27.11 | ...    | plot computation | plot computation | website structure | website structure |
| 04.12 |        |                  |                  |                   |                   |
| 21.12 |        |                  |                  |                   |                   |

## Questions for TAs
