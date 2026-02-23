# Standardization

*   All the query for the perticlar requests type will be written seperately in side a seperate file with <request_file_name>db.
*   The request file and the db file for the corresponding should be in a seperate folder
      example - /handle_user
                    user_handler.py
                    user_handlerdb.py
       
*   The function that can be broken down into more function witch two different tasks will be divided to create modularity and readability
*   The variables name should be self explanatory of what is that exactly storing like
    -   userdb
    -   user_id
    -   session etc
