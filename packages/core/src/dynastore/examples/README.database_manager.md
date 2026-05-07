# DatabaseManager Extension

Manages the lifecycle of the database connection for DynaStore, injecting a DB client into the FastAPI app state.

## Features
- On startup: Opens a database connection and attaches it to `app.state.db_client`.
- On shutdown: Closes the database connection.

## Usage
This extension does not expose API routes but manages DB resources for other extensions.

## Example
```python
# Access the DB client in your endpoint
@app.get("/some-endpoint")
def some_endpoint(request: Request):
    db_client = request.app.state.db_client
    ...
```

## Author
Carlo Cancellieri
