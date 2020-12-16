from pysirix import sirix_async, Sirix, DBType, JsonStoreAsync, Resource
import httpx

httpx_client = httpx.AsyncClient(base_url="http://localhost:9443")


async def get_sirix():
    return await sirix_async("admin", "admin", httpx_client)


def get_json_store(sirix: Sirix) -> JsonStoreAsync:
    return sirix.database("contacts", DBType.JSON).json_store("contacts")


def get_json_resource(sirix: Sirix) -> Resource:
    return sirix.database("contacts", DBType.JSON).resource("contacts")
