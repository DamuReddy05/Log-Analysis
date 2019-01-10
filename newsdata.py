#!/usr/bin/env python3
import psycopg2
# importing for Proper date format
from datetime import datetime
# Query for finding 'What are the most popular three articles of all time?'
query1 = (
      "select a.title, count(*) as views "
      "from articles a inner join log b "
      "on a.slug = replace(path,'/article/','') "
      "where status='200 OK' and length(path)>1 group by "
      "a.title order by views desc limit 3")


# Query for searching 'Who are the most popular article authors of all time?'
query2 = (
      "select c.name, count(*) as views "
      "from articles a inner join log b on "
      "a.slug = replace(path,'/article/','') inner join "
      "authors c on (c.id=a.author) "
      "where status='200 OK' and length(path)>1 group by "
      "c.name order by views desc")

# Query for finding 'On which days did more than 1% of requests lead to errors'
query3 = (
      "select day, perc from ("
      "select day, round((sum(requests)/(select count(*) from log where "
      "substring(cast(log.time as text), 0, 11) = day) * 100), 2) as "
      "perc from (select substring(cast(log.time as text), 0, 11) as day, "
      "count(*) as requests from log where status like '%404%' group by day)"
      "as log_percentage group by day order by perc desc) as final_query "
      "where perc >= 1")


# Providing Database Connection and executing query
def get_query_results(query):
    # Return results
    dbName = "news"
    db = psycopg2.connect("dbname={}".format(dbName))
    cursor = db.cursor()
    cursor.execute(query)
    return cursor.fetchall()
    db.close()


# Printing query results
def print_query_results(query_results):
    for i, rs in enumerate(query_results):
        print ("\t"+str(i+1)+"."+str(rs[0])+" - "+str(rs[1])+" views")


# Method for Printing erorr result
def print_error_results(query_results):
    for results in query_results:
        d = results[0]
        date_obj = datetime.strptime(d, "%Y-%m-%d")
        formatted_date = datetime.strftime(date_obj, "%B %d, %Y")
        print ("\t"+str(formatted_date)+" - "+str(results[1]) + "% errors")


if __name__ == '__main__':
    print("What are the most popular three articles of all time?")
    # get query1 results
    popular_articles = get_query_results(query1)
    # print query1 results
    print_query_results(popular_articles)
    print("Who are the most popular article authors of all time?")
    # get query2 results
    popular_authors = get_query_results(query2)
    # print query2 results
    print_query_results(popular_authors)
    print("On which days did more than 1% of requests lead to errors?")
    # get query3 results
    error_days = get_query_results(query3)
    # print query3 results
    print_error_results(error_days)
