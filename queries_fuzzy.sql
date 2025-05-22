-- FUZZY MATCHING WITH TRIGRAMS

-- ok so this is great...
select title, ts_rank(title_search, websearch_to_tsquery('rush'), 1) as rank
from movies
where title_search @@ plainto_tsquery('rush hour')
order by rank desc; -- yields 2 movies, Rush Hour 2 and 3

-- but what if our dear user has phat fingers
select title, ts_rank(title_search, websearch_to_tsquery('russh hour'), 1) as rank
from movies
where title_search @@ plainto_tsquery('russh hour')
order by rank desc; -- No movies for you!

-- We will need to do some 'fuzzy' matching.. Trigrams are an algorithm that can do fuzzy matching with similarity.  Postgres has an extension for this
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- We will need a source of unique true words from our movie db in order to make suggestions when things are misspelled or where there multiple spellings
CREATE TABLE "movie_word" (
    "word" TEXT NOT NULL
);

-- Words should of course be unique
CREATE UNIQUE INDEX "movie_word_word_key" ON "movie_word"("word");

-- we will need an index to make similarity searches fast
CREATE INDEX "movie_word_idx" ON "movie_word" USING GIN ("word" gin_trgm_ops);

-- the words will need to be updated as we get new movies so lets put the code to build the word index into a procedure so we can run it periodically
CREATE OR REPLACE FUNCTION ft_refresh_movie_word ()
RETURNS void
AS $$
BEGIN
    -- Truncate the table to remove all rows
    TRUNCATE TABLE movie_word;

    -- Insert words from the product table into the product_words table
    INSERT INTO movie_word (word)
    SELECT word
    FROM ts_stat('SELECT to_tsvector(''simple'', title) || to_tsvector(''simple'', original_title) FROM movies');	-- the 'simple' ensures the true words from the movies table go into the index, not stemmed

    -- No need to explicitly return anything since the function is void
END;
$$
LANGUAGE plpgsql;

-- do an initial build of the word index
select ft_refresh_movie_word();

--lets have a look at the word index
select * from movie_word limit 1000;

-- in order to cope with misspellings, we will need to pre-process our query text and compile a query with substitution options.. lets start with a utility function that does this... this is roughly like a custom version of to_tsquery
CREATE OR REPLACE FUNCTION ft_prep_movie_query (search_text text)
	RETURNS text
	AS $$
DECLARE
	search_word text;
	substituted_words text[] := '{}'; -- Array to hold words to be used in tsquery
	similar_words text[];
	qout text := ''; -- Final tsquery to be built
	index integer := 0;
BEGIN
	-- Loop through each word in the input search_text
	FOREACH search_word IN ARRAY string_to_array(search_text, ' ')
	LOOP
		SELECT
			array_agg(word) INTO similar_words
		FROM (
			SELECT
				word
			FROM
				movie_word
			WHERE
				word % search_word -- Fuzzy match (%) using pg_trgm, essentially find words that are similar to each search word
			ORDER BY similarity (word, search_word) DESC
			LIMIT case when index = 0 then 3 else 6 end -- You will probably need to tweak this for your use case.  I found that 6 alternates is fine if you have 2 or more words in the query but for single word queries, this gets a little whacky so I dialled it down to 3 alternates.  I couldn't figure out how to make the terms in the OR list to have different weights, obviously the original word should be the preferred match and ideally would bump the rank somehow..
) AS subquery;
		similar_words := array_prepend(search_word, similar_words);
		substituted_words := array_append(substituted_words, concat('(', array_to_string(similar_words, ' | '), ')')); -- join similar word with OR
		
		index := index + 1;
	END LOOP;
	-- Build the final tsquery with AND operator
	qout := array_to_string(substituted_words, ' & ');	-- Depending on your use case, you may consider using the 'following' clause here instead (' <-> ')

	-- Return the final query as text
	RETURN qout;
END;
$$
LANGUAGE plpgsql;

-- Now we can see that out query terms are compiled into groups of alternate spellings joined with an OR (|) clause
select ft_prep_movie_query('star warps'); -- (star | stay | superstar | station) & (warps | war | wars | warm | ward | d-war)
select ft_prep_movie_query('russh hour'); -- (russh | rush | russia | rust) & (hour | hour | hours | house | ho)


-- Now we want to actually run some queries but we still have some work to do...

-- Counterintuitively, this does NOT work... I'm still trying to figure out why.  I think it's something to do with statement parsing
SELECT * from movies where title_search @@ to_tsquery(ft_prep_movie_query ('russh hour'));

-- We essentially need to prep the query first and then use it.. without resorting to plpgsql, we need to do this with a temp table
WITH prep AS (
	SELECT
		to_tsquery(ft_prep_movie_query ('russh hour')) AS query
)
select title, ts_rank(title_search, prep.query, 1) as rank
from movies, prep
where title_search @@ query
order by rank desc; -- Rush Hour 2 and 3 are back! WB Jackie Chan

--This is a bit clumsy so lets get jiggy with plpgsql and build a search procedure...
CREATE OR REPLACE FUNCTION ft_movie_search (search_text text)
	RETURNS TABLE (
		title text,
		rank real
	)
	AS $$
DECLARE
	search_text_sub text := ''; -- input search subbed with alt match groups
	tsquery_sub tsquery; 				-- tsquery with subbed search
BEGIN
	search_text_sub := ft_prep_movie_query (search_text);
	tsquery_sub := to_tsquery(search_text_sub);
	RETURN QUERY
	SELECT
		m.title,
		ts_rank(m.title_search, tsquery_sub) AS rank
	FROM
		movies m
	WHERE
		m.title_search @@ tsquery_sub
	ORDER BY
		ts_rank(m.title_search, tsquery_sub) DESC;
	RETURN;
END;
$$
LANGUAGE plpgsql;

-- Here you go, full text search with stemming etc from tsQuery and tsVector combined with fuzzy matching from pg_trgm
-- these work great
select * from ft_movie_search('russh hour');
select * from ft_movie_search('Match Poimt');

-- OOPS.... these do not work... I think because they have words that can be stemmed... think there is something wrong with the title_search column
select * from ft_movie_search('star wars');
select * from ft_movie_search('Four Rooms');
select * from ft_movie_search('American Beauty');
select * from ft_movie_search('Apocalypse Now');

-- investigating....
select *  from movies where title = 'Star Wars'; -- works
select * from movies where to_tsvector(title) @@ plainto_tsquery('star wars'); -- works
select * from movies where title_search @@ plainto_tsquery('star wars'); -- does not work

-- still investigating
select 
	title,
	title_search,
	to_tsvector(title) as to_tsvector,
	plainto_tsquery('star wars') as plain_to_query,
	ts_rank(to_tsvector(title), plainto_tsquery('star wars')) as rank_manual,
	ts_rank(title_search, plainto_tsquery('star wars')) as rank_calculated	
from movies where title = 'Star Wars';
/*
title	    title_search	            to_tsvector	      plain_to_query	rank_manual	rank_calculated
Star Wars	'star':1A,3B 'wars':2A,4B	'star':1 'war':2	'star' & 'war'	0.09910322	1e-20

aha, the title_search isn't stemmed because you used 'simple' in the generated column... I think this is a mistake and gives inconsistent results

So it turns out you can't just drop the 'simple' entirely because then the call to to_tsvector is not 'immutable' and the Add fails

I decided to compromise and use 'english' which is both immutable and stems.... alternatively you *could* create a permanent column and populate it with a trigger (which is what I did in my impl)
*/

alter table movies drop title_search;
drop index idx_search;

alter table movies 
add title_search tsvector 
generated always as	(
	setweight(to_tsvector('english', coalesce(title, '')), 'A') || ' ' || 
	setweight(to_tsvector('english', coalesce(original_title, '')), 'B') :: tsvector
) stored; 

create index idx_search on movies using GIN(title_search);

-- ok retest
select * from ft_movie_search('star warps'); -- Luke is back!
