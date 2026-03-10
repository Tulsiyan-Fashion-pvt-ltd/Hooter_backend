# Standardization

*   All the query for the perticlar requests type will be written seperately in side a seperate file with <request_file_name>db.
*   The request file and the db file for the corresponding should be in a seperate folders
      example - the routes will be under /api_routes foleder and 
              - sql queries will be under sql_queries folder
       
*   The function that can be broken down into more function witch two different tasks will be divided to create modularity and readability
*   The variables name should be self explanatory of what is that exactly storing like
    -   userdb
    -   user_id
    -   session etc

## sessions
    - session['user'] => stores the user session
    - session['brand'] => stores the brand id as the connected brand to that user on that particular session
