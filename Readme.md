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

## Catalog

    Image storage:
    Image are stored inside the hidden folder `.product_images/`

Images are stored in 3 different folders inside `.product_images/` as `.oringal_images`, `.high_resol_images` and `.low_resol_images`

### Image Filename Formate
File are stored in the sepecific formate where it is devided using `_-_`\
*Example:* usku_00123_-_front.png\
\
Here the usku_00123 is **usku_id** and **front** is the image type and **.png** is the file extension.

