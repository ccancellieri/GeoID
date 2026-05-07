from dynastore.extensions.protocols import ExtensionProtocol
from contextlib import asynccontextmanager 
from fastapi import FastAPI

# A mock database client for the example
class MockDBClient:
    def connect(self): print("DB connection opened. 🐘")
    def close(self): print("DB connection closed. 🐘")
class DatabaseManager(ExtensionProtocol):
    # No router attribute defined!
    
    @staticmethod
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Creates a DB client and shares it in the app's state."""
        print("DatabaseManager starting up...")
        db_client = MockDBClient()
        db_client.connect()
        app.state.db_client = db_client # Inject the client into the app state
        try:
            yield
        finally:
            app.state.db_client.close()
            print("DatabaseManager shut down.")