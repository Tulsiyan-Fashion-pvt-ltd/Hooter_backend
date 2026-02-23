*  Farhan -> 23 feb 2026

# switched from wsgi to asgi
  We have switched from wsgi to asgi server using [quart](https://pypi.org/project/Quart/0.13.0/) instead of [flask](https://pypi.org/project/Flask/). Which means now we are taking the advantage of asynchronous programming.
  We have to use async and await built in keywords from python to make the functions asynchronous. Any I/O bound operation should be done under async function either writing database queries or dealing with apis.

  ## simple operations don't need to be async functions like calculations, id creation which doesn't need to awaited
  See the example in the /handle_user/user_handler.py

  ```
  # request to fetch user session
  @handle_user.route('/session', methods=['GET'])
  async def check_session():
      user = session.get('user')
      # print(user)
      if user:
          return jsonify({'login': 'ok'}), 200
      else:
          return jsonify({'login': 'deny'}), 401
  ```

  Since we have switched from wsgi to asgi , we are using [asyncmy](https://pypi.org/project/asyncmy/) for handling the sql connections asynchronously using connecion pool. 

The pool starts before starting the service and stores in app
```
@app.before_serving  
async def sql_connection_startup():
    app.pool= await asyncmy.create_pool(
        host = os.environ.get('HOOTER_DB_HOST'),
        port = int(os.environ.get('HOOTER_DB_PORT')),
        user = os.environ.get('HOOTER_DB_USER'),
        password = os.environ.get('HOOTER_DB_PASSWORD'),
        db = os.environ.get('HOOTER_DB'),
        minsize = 1,
        maxsize = 20
    )
```

So in order to use the pool,use current_app.app by calling current_app from quart as `from quart import current_app`.
And use DictCursor from asyncmy.cursors
The flow would be normal as -
```
from quart import current_app
from asyncmy.cursors import DictCursor


async def fetch_user_name():
  pool = current_app.pool
  async with pool.acquire() as connection:
    async with connection.cursor(cursor=DictCursor) as cursor:
      await cursor.execute('''SELECT user_name FROM users''')
      user_name = await cursor.fetchone()
      return user_name
```

See above, no need to close the pool since it auto closes itself on `@app.after_serving`

