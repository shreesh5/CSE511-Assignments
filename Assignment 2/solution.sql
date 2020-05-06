CREATE TABLE query1 AS
SELECT g.name, count(1) as moviecount
FROM hasagenre
         JOIN genres g USING (genreid)
GROUP BY name;

CREATE TABLE query2 AS
SELECT g.name AS name, avg(rating) AS rating
FROM movies m
         JOIN hasagenre h USING (movieid)
         JOIN ratings r USING (movieid)
         JOIN genres g USING (genreid)
GROUP BY g.name;

CREATE TABLE query3 AS
SELECT m.title, count(1) AS countofratings
FROM ratings
         JOIN movies m USING (movieid)
GROUP BY m.title
HAVING count(1) > 9;

CREATE TABLE query4 AS
SELECT m.movieid AS movieid, m.title
FROM movies m
         JOIN hasagenre h USING (movieid)
         JOIN genres g USING (genreid)
WHERE g.name = 'Comedy';

CREATE TABLE query5 AS
SELECT m.title, avg(rating) as average
FROM ratings r
         JOIN movies m USING (movieid)
GROUP BY m.title;

CREATE TABLE query6 AS
SELECT avg(r.rating) AS average
FROM movies m
         JOIN ratings r USING (movieid)
         JOIN hasagenre h USING (movieid)
         JOIN genres g USING (genreid)
WHERE g.name = 'Comedy';

CREATE TABLE query7 AS
SELECT avg(rating) as average
FROM ratings
        JOIN (SELECT movieid
			  FROM movies m
					JOIN hasagenre h USING (movieid)
					JOIN genres g USING (genreid)
			  WHERE g.name IN ('Romance', 'Comedy')
			  GROUP BY m.movieid
			  HAVING count(1) = 2) Y
		USING (movieid);

CREATE TABLE query8 AS
SELECT avg(rating) as average
FROM ratings
         JOIN (SELECT movieid
			   FROM movies m
					JOIN hasagenre h USING (movieid)
					JOIN genres g USING (genreid)
			   WHERE g.name IN ('Romance', 'Comedy')
			   GROUP BY m.movieid
			   HAVING array_agg(g.name) = '{Romance}') Y
     USING (movieid);

CREATE TABLE query9 AS
SELECT movieid, rating
FROM ratings
WHERE ratings.userid = :v1;

CREATE VIEW AvgTab AS
SELECT movieid, AVG(rating) AS rating
FROM ratings
GROUP BY movieid;

CREATE VIEW similarities AS 
SELECT m1.movieid AS movieid1, m2.movieid AS movieid2, (1-ABS(m1.rating - m2.rating)/5) AS sim
FROM AvgTab m1, AvgTab m2
WHERE m1.movieid <> m2.movieid;

CREATE TABLE recommendation AS
SELECT m.title
FROM (SELECT movieid2 i, sim, r1.rating
      FROM similarities s
               JOIN ratings r1 ON s.movieid1 = r1.movieid AND userid = :v1
               LEFT JOIN (SELECT movieid FROM ratings WHERE userid = :v1) r2 ON (s.movieid2 = r2.movieid)
      WHERE r2.movieid ISNULL
     ) X
         JOIN movies m ON (i = movieid)
GROUP BY m.movieid, m.title
HAVING (sum(X.sim * X.rating) / sum(X.sim)) > 3.9
ORDER BY m.title;