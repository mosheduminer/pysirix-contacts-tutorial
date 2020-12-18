import pysirix
import httpx


def init(database_name: str, resource_name: str):
    client = httpx.Client(base_url="http://localhost:9443")
    sirix = pysirix.sirix_sync("admin", "admin", client)
    store = sirix.database(database_name, pysirix.DBType.JSON).json_store(resource_name)
    if not store.exists():
        # database will be created implicitly if it does not exist when the resource is created
        store.create()
        print(f"created resource {resource_name} in database {database_name}")
    else:
        print(f"resource {resource_name} in database {database_name} already exists")
    client.close()


if __name__ == "__main__":
    init("contacts", "contacts")
