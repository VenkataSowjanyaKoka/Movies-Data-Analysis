```python
#for structured data we use Spark SQL, SparkSession acts a pipeline between data and statements accessing the data
from pyspark.sql import SparkSession 
```


```python
# sparksession is like a class and we need to create an instance of a class to utilize the class
spark = SparkSession.builder.appName("Movies Data Processing").getOrCreate()
```


```python
#Reading the CSV File as a dataframe
#if infer schema is given false it expects to give schema
#if header is given false it takes its own column names like c_0,c_1...
Movies_DF = spark.read.csv("/Users/sowjanyakoka/Desktop/Spring2020/MachineLearning/movies.csv", inferSchema=True,header=True)
```


```python
#Looking into the complete dataset
Movies_DF.show(5)
```

    +-----------------+-------------+----+
    |            actor|        title|year|
    +-----------------+-------------+----+
    |McClure, Marc (I)|Freaky Friday|2003|
    |McClure, Marc (I)| Coach Carter|2005|
    |McClure, Marc (I)|  Superman II|1980|
    |McClure, Marc (I)|    Apollo 13|1995|
    |McClure, Marc (I)|     Superman|1978|
    +-----------------+-------------+----+
    only showing top 5 rows
    



```python
#1.How many movies are included in movies.csv?
# Number of movies included can be known by seeing the number of obsdervations or records(rows) in the dataframe
Number_Of_Movies = Movies_DF.count()
print("Number of movies included in movies.csv are :", Number_Of_Movies)
```

    Number of movies included in movies.csv are : 31394



```python
#2.What features (or attributes) are recorded for each movie?
#The features recorded for each movie can be known by the column names in the dataframe
Movie_Features = Movies_DF.columns
print("The features recorded for each movie are :", Movie_Features)
```

    The features recorded for each movie are : ['actor', 'title', 'year']



```python
#3.What is the shape of your data?
#The shape of the data is the number of rows and columns (m = rows, n = columns) present in the dataset
print("Shape :",(Movies_DF.count(),len(Movies_DF.columns)))
```

    Shape : (31394, 3)



```python
#4.Provide a schema of the movies data set.
#Schema is the outline of the dataframe which gives us an outline(column_name, datatype, possibility of null values) of each column in the dataset
print("The schema of the movies data set is:")
#Displaying the schema of the dataset
Movies_DF.printSchema()
```

    The schema of the movies data set is:
    root
     |-- actor: string (nullable = true)
     |-- title: string (nullable = true)
     |-- year: integer (nullable = true)
    



```python
#5.Provide a listing of the first 5 movies. For each movie, display the movie name and year it was produced, in that order.
#Movie_Year_DF is a dataframe with only movie name and year it was produced columns 
Movie_Year_DF = Movies_DF['title','year']
print("Listing of the first 5 movies :")
#displaying the movie name and year it was produced for first 5 movies
Movie_Year_DF.show(5)
```

    Listing of the first 5 movies :
    +-------------+----+
    |        title|year|
    +-------------+----+
    |Freaky Friday|2003|
    | Coach Carter|2005|
    |  Superman II|1980|
    |    Apollo 13|1995|
    |     Superman|1978|
    +-------------+----+
    only showing top 5 rows
    



```python
#6.Provide a count of all movies produced before the year 2000.
#Movies_Before_2000_DF is a dataframe that has movies produced only before year 2000
Movies_Before_2000_DF = Movies_DF[Movies_DF['year'] < 2000 ]
print("The count of all movies produced before the year 2000 is :",Movies_Before_2000_DF.count())
```

    The count of all movies produced before the year 2000 is : 8400



```python
#7.List the names of all actors who have acted in the movie Contagion. List only the names of actor.
#Contagion_Movie_DF is a dataframe with details related to only Contagion Movie
Contagion_Movie_DF = Movies_DF.filter(Movies_DF['title'] == "Contagion")
#Contagion_Movie_Actors_DF is a dataframe with only actor names who acted in Contagion Movie
Contagion_Movie_Actors_DF = Contagion_Movie_DF.select('actor')
print("List of names of all actors who have acted in the movie Contagion is :")
#Displaying only the names of actors
Contagion_Movie_Actors_DF.orderBy('actor',ascending = True).show(Contagion_Movie_Actors_DF.count())
```

    List of names of all actors who have acted in the movie Contagion is :
    +--------------------+
    |               actor|
    +--------------------+
    |     Armour, Annabel|
    | Bartlett, Tommy (I)|
    |Chamberlain, Cabr...|
    |       Clarke, Larry|
    |Cohen, David (XXV...|
    |   Colantoni, Enrico|
    |   Cotillard, Marion|
    |     Cranston, Bryan|
    |Curnen, Monique G...|
    |         Damon, Matt|
    |Edwards, Shannon (I)|
    |      Ehle, Jennifer|
    | Fishburne, Laurence|
    |      Gould, Elliott|
    |    Hawkes, John (I)|
    |Kanellakos, Alexa...|
    |          Kress, Don|
    |       Lathan, Sanaa|
    |        Lavell, Mark|
    |           Law, Jude|
    |        Marino, Carl|
    |     Meadows, Samuel|
    |  Mitchell, E. Roger|
    |      Nazimek, Larry|
    |        Ortlieb, Jim|
    |    Paltrow, Gwyneth|
    |Panzarella, Russ (V)|
    | Price, Steven James|
    |Smith, Gregory Ma...|
    | Spence, Rebecca (I)|
    |      Stern, January|
    |  Stewart, Thomas W.|
    |    Thomas, Chris D.|
    |       Thurner, John|
    | Weston II, James D.|
    |      Wiggins, Roger|
    |       Winslet, Kate|
    |    Young, Robert A.|
    +--------------------+
    



```python
#8.List all movies in which John Travolta has acted. List the movie name and year produced.
#John_Travolta_Movies_DF is a dataframe with details of the movies in John_Travolta has acted
John_Travolta_Movies_DF = Movies_DF.filter(Movies_DF['actor']=='Travolta, John')
print("All movies in which John Travolta has acted :")
#Displaying all movies in which John Travolta has acted
John_Travolta_Movies_DF['title','year'].show(John_Travolta_Movies_DF.count())
```

    All movies in which John Travolta has acted :
    +--------------------+----+
    |               title|year|
    +--------------------+----+
    |          Phenomenon|1996|
    |           Wild Hogs|2007|
    |             Michael|1996|
    |Saturday Night Fever|1977|
    |           Hairspray|2007|
    |              Grease|1978|
    |      Primary Colors|1998|
    |        Pulp Fiction|1994|
    |   The Thin Red Line|1998|
    |           Swordfish|2001|
    |      A Civil Action|1998|
    |Magnificent Desol...|2005|
    |  Look Who's Talking|1989|
    |Austin Powers in ...|2002|
    |Domestic Disturbance|2001|
    |The General's Dau...|1999|
    |            Face/Off|1997|
    |                Bolt|2008|
    |           Ladder 49|2004|
    |The Taking of Pel...|2009|
    +--------------------+----+
    



```python
#9.Provide a count of movies each actor has acted in (group by actor’s name). The list should display actor’s name and count of movies. The list should be alphabetized by actor’s name.
#Calculating the number of movies acted by each actor and sorting it by the name of the actor and displaying them
Movies_DF.groupBy('actor').count().orderBy('actor',ascending = True).show(10)

```

    +----------------+-----+
    |           actor|count|
    +----------------+-----+
    | Aaron, Caroline|    6|
    |  Aarons, Bonnie|    5|
    | Abadie, William|    3|
    | Abbott, Deborah|    4|
    |     Abdoo, Rose|    5|
    |  Abdullah, Haji|    5|
    | Abell, Alistair|    3|
    |Abercrombie, Ian|    5|
    |Abergel, Rakefet|    3|
    |  Abernathy, Don|   18|
    +----------------+-----+
    only showing top 10 rows
    



```python
#10.Create a standard pyspark UDF to designate any movie produced before 2000 as Old while movies produced in 2000 or later are designated as New.
#Then, apply the UDF to the movies data so a new column named ‘age_category’ is added permanently.
#Display the first 10 movie records (Movie name, year produced, age_category).
#for creating a UDF using pyspark importing the required libraries
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType
#Classifying movies based on age_category using user defined function
age_category_udf = udf(lambda year: 'Old' if year < 2000 else 'New', StringType())
#Applying User Defined Function to a column in our data and creating a new DataFrame 
New_Movies_DF = Movies_DF.withColumn('age_category', age_category_udf(Movies_DF.year))
print("The first 10 movie records (Movie name, year produced, age_category) :")
#Displaying the first 10 records
New_Movies_DF['title','year','age_category'].show(10)
```

    The first 10 movie records (Movie name, year produced, age_category) :
    +--------------------+----+------------+
    |               title|year|age_category|
    +--------------------+----+------------+
    |       Freaky Friday|2003|         New|
    |        Coach Carter|2005|         New|
    |         Superman II|1980|         Old|
    |           Apollo 13|1995|         Old|
    |            Superman|1978|         Old|
    |  Back to the Future|1985|         Old|
    |Back to the Futur...|1990|         Old|
    |  Me, Myself & Irene|2000|         New|
    |         October Sky|1999|         Old|
    |              Capote|2005|         New|
    +--------------------+----+------------+
    only showing top 10 rows
    



```python

```
