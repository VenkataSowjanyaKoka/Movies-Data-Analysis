# Movies-Data-Analysis
Exploratory Data Analysis Using PySpark

Using the data contained in the movies.csv file to prepare answers for the following questions.
Written the necessary python statements in pyspark and jupyter notebook to obtain answers.

**Questions to be Answered**

1.	How many movies are included in movies.csv?
2.	What features (or attributes) are recorded for each movie?
3. 	What is the shape of your data?
4.	Provide a schema of the movies data set. 
5.	Provide a listing of the first 5 movies. For each movie, display the movie name and year it was produced, in that order.
6.	Provide a count of all movies produced before the year 2000.
7.	List the names of all actors who have acted in the movie Contagion. List only the names of actor. 
8.	List all movies in which John Travolta has acted. List the movie name and year produced. 
9.	Provide a count of movies each actor has acted in (group by actor’s name). The list should display actor’s name and count of movies. The list should be alphabetized by actor’s name. 
10.	Create a standard pyspark UDF to designate any movie produced before 2000 as Old while  movies produced in 2000 or later are designated as New. Then, apply the UDF to the movies data so a new column named ‘age_category’ is added permanently. Display the first 10 movie records (Movie name, year produced, age_category).

