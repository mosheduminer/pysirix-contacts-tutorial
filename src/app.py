from datetime import datetime
from typing import Union, Optional

from fastapi import FastAPI, Depends, status, Response, HTTPException
from pysirix import (
    sirix_async,
    DBType,
    JsonStoreAsync,
    Resource,
    SirixServerError,
)
import httpx
from . import schemas

app = FastAPI()


async def get_json_store() -> JsonStoreAsync:
    httpx_client = httpx.AsyncClient(base_url="http://localhost:9443")
    sirix = await sirix_async("admin", "admin", httpx_client)
    store = sirix.database("contacts", DBType.JSON).json_store("contacts")
    try:
        yield store
    finally:
        sirix.dispose()
        await httpx_client.aclose()


async def get_json_resource() -> Resource:
    httpx_client = httpx.AsyncClient(base_url="http://localhost:9443")
    sirix = await sirix_async("admin", "admin", httpx_client)
    resource = sirix.database("contacts", DBType.JSON).resource("contacts")
    try:
        yield resource
    finally:
        sirix.dispose()
        await httpx_client.aclose()


# alternatively, we can do this without creating a new httpx_client and
# without creating a new ``Sirix``` instance (which also has the extra
# overhead of authenticating every time ``sirix_async`` is called
#
#
# httpx_client = httpx.AsyncClient(base_url="http://localhost:9443")
#
# sirix = Sirix("admin", "admin", httpx_client)
#
#
# @app.on_event("startup")
# async def init_sirix():
#     await sirix.authenticate()
#
#
# def get_json_store() -> JsonStoreAsync:
#     return sirix.database("contacts", DBType.JSON).json_store("contacts")
#
#
# def get_json_resource() -> Resource:
#     return sirix.database("contacts", DBType.JSON).resource("contacts")


def parse_revision(
    revision_id: Union[int], revision_timestamp: Union[str, None]
) -> Union[int, datetime, None]:
    """
    A utility function to return either a revision ID or a revision timestamp, or ``None``,
            given two possible values (``revision_id`` and ``revision_timestamp``).

    :return: an ``int`` or ``datetime`` representing a revision 
    """
    return revision_id or (
        (
            revision_timestamp
            and datetime.strptime(revision_timestamp, "%Y-%m-%dT%H:%M:%S.%f")
        )
        or None
    )


@app.post("/contact/new", status_code=status.HTTP_204_NO_CONTENT)
async def new_contact(
    contact: schemas.Contact, json_store: JsonStoreAsync = Depends(get_json_store)
):
    """
    Create a new contact
    """
    await json_store.insert_one(contact.dict())


@app.get("/contact/list", response_model=list[schemas.ContactWithMeta])
async def list_contacts(
    revision_id: Optional[int] = None,
    revision_timestamp: Optional[str] = None,
    json_store: JsonStoreAsync = Depends(get_json_store),
):
    """
    List all contacts. Optionally, a revision may be specified.
    """
    results = await json_store.find_all(
        {}, revision=parse_revision(revision_id, revision_timestamp), hash=True
    )
    return [
        schemas.ContactWithMeta(**result, key=result["nodeKey"]) for result in results
    ]


@app.post("/contact/search", response_model=list[schemas.ContactWithMeta])
async def search_contacts(
    query_terms: list[schemas.QueryTerm],
    revision_id: Optional[int] = None,
    revision_timestamp: Optional[str] = None,
    resource: Resource = Depends(get_json_resource),
):
    """
    Search for a contact. If an empty list is provided instead of a list of
            search terms, a 400 error is returned
    Provide a `revision_id` or `revision_timestamp` to search a particular revision,
    instead of the latest.
    """
    if len(query_terms) == 0:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "when not using search terms, use the /contact/list endpoint instead",
        )
    if revision_id:
        open_resource = f"jn:doc('contacts','contacts', {revision_id})"
    elif revision_timestamp:
        open_resource = (
            f"jn:open('contacts','contacts', xs:dateTime('{revision_timestamp}'))"
        )
    else:
        open_resource = "."
    query_list = []
    for query_term in query_terms:
        if query_term.fuzzy:
            query_list.append(
                f"(typeswitch($i=>{query_term.field}) "
                f"case xs:string return contains(xs:string($i=>{query_term.field}), '{query_term.term}')"
                " default return false())"
            )
        else:
            query_list.append(f"$i=>{query_term.field} eq '{query_term.term}'")
    query_filter = " and ".join(query_list)
    query = (
        f"for $i in bit:array-values({open_resource}) where {query_filter}"
        " return {$i, 'nodeKey': sdb:nodekey($i), 'hash': sdb:hash($i)}"
    )
    results = await resource.query(query)
    return [
        schemas.ContactWithMeta(**result, key=result["nodeKey"])
        for result in results["rest"]
    ]


@app.post("/contact/search/all-time", response_model=list[schemas.HistoricalContact])
async def search_contacts_all_time(
    query_terms: list[schemas.QueryTerm],
    existing: bool = True,
    resource: Resource = Depends(get_json_resource),
):
    """
    Search for a contact, even it does not currently exist.
    All contacts are returned if an empty list is provided.

    If `existing` is `True`, then currently existing contacts will be returned as well.
    """
    if len(query_terms) == 0:
        query_filter = ""
    else:
        query_list = []
        for query_term in query_terms:
            if query_term.fuzzy:
                query_list.append(
                    f"(typeswitch($i=>{query_term.field}) "
                    f"case xs:string return contains(xs:string($i=>{query_term.field}), '{query_term.term}')"
                    " default return false())"
                )
            else:
                query_list.append(f"$i=>{query_term.field} eq '{query_term.term}'")
        query_filter = " and ".join(query_list)
        query_filter = f"where {query_filter}"
    query_deleted_only = "where sdb:is-deleted($i)" if not existing else ""
    deduplicate = (
        "if (not(exists(jn:future($i)))) then $i "
        "else if (sdb:hash($i) ne sdb:hash(jn:future($i))) then $i "
        "else ()"
    )
    query = (
        "for $rev in jn:all-times(.) for $i in bit:array-values($rev) "
        f"{query_deleted_only} {query_filter} return {deduplicate}"
    )
    results = await resource.query(query)
    return results["rest"]


@app.get("/contact/{contact_key}", response_model=schemas.Contact)
async def view_contact(
    contact_key: int,
    revision_id: Optional[int] = None,
    revision_timestamp: Optional[str] = None,
    json_store: JsonStoreAsync = Depends(get_json_store),
):
    """
    Return a contact, given its key. Can return the contact as it was in different points in time.
    By default, the current version is returned.
    """
    result = await json_store.find_by_key(
        contact_key, parse_revision(revision_id, revision_timestamp)
    )
    return schemas.Contact(**result)


@app.get(
    "/contact/{contact_key}/history",
    response_model=Union[list[schemas.HistoricalContact], list[schemas.Revision]],
)
async def view_contact_history(
    contact_key: int,
    revision_id: Optional[int] = None,
    revision_timestamp: Optional[str] = None,
    embed: bool = False,
    json_store: JsonStoreAsync = Depends(get_json_store),
):
    """
    Return the history of a contact, given its key.
    If `embed` if `False`, then only the metadata of each revision will be returned.
    Else, the contact (as it was at that revision) will be returned as well.

    If the contact does not currently exist, a `revision_id` or `revision_timestamp`
    of when the contact _did_ exist can be supplied.
    """
    if embed:
        results = await json_store.history_embed(
            contact_key, parse_revision(revision_id, revision_timestamp)
        )
        return [schemas.HistoricalContact(**result) for result in results]
    else:
        return await json_store.history(
            contact_key, revision=parse_revision(revision_id, revision_timestamp)
        )


@app.put("/contact/{contact_key}", status_code=status.HTTP_204_NO_CONTENT)
async def update_contact(
    contact_key: int,
    contact: schemas.Contact,
    json_store: JsonStoreAsync = Depends(get_json_store),
):
    """
    Update a contact. Fields in the new contact object will
            overwrite fields in the old version of the contact.
    """
    await json_store.update_by_key(contact_key, contact.dict())


@app.delete("/contact/{contact_key}")
async def delete_contact(
    contact_key: int, hash: str, resource: Resource = Depends(get_json_resource)
):
    """
    Delete the contact with the given key.
    If the record has changed since the hash was obtained, a 409 error is returned.
    """
    try:
        await resource.delete(contact_key, hash)
    except SirixServerError:
        return Response(status_code=status.HTTP_409_CONFLICT)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
