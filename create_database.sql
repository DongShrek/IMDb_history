create table mydb.imdb_history
(
    `Rank`   text null,
    Rating   text null,
    Title    text null,
    Votes    text null,
    Day      text null,
    MovieID  text null,
    IMDbPage text null
);

create table mydb.movie_basic_info
(
    `index`  bigint null,
    `0`      text   null,
    Title    text   null,
    Country  text   null,
    Language text   null,
    Director text   null,
    Genre    text   null,
    kind     text   null,
    Year     bigint null
);

create index ix_movie_basic_info_index
    on mydb.movie_basic_info (`index`);