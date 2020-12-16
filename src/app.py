from datetime import datetime
from typing import Union, Optional

from fastapi import FastAPI, Depends, status, Response
from pysirix import Sirix, SirixServerError, Insert

from depends import get_json_resource, get_sirix, get_json_store
import schemas


app = FastAPI()


@app.post("/search", response_model=list[schemas.ContactWithKey])
async def search_contacts(
    query_terms: list[schemas.QueryTerm],
    revision_id: Optional[int] = None,
    revision_timestamp: Optional[str] = None,
    sirix: Sirix = Depends(get_sirix),
):
    contacts_resource = get_json_store(sirix)
    results = await contacts_resource.find_all(
        {query_term.term: query_term.field for query_term in query_terms},
        revision=revision_timestamp or revision_id,
    )
    return [schemas.ContactWithKey(result, key=result["nodeKey"]) for result in results]


@app.get("/contact/{contact_key}", response_model=schemas.ContactWithKey)
async def view_contact(
    contact_key: int,
    revision_id: Optional[int] = None,
    revision_timestamp: Optional[str] = None,
    sirix: Sirix = Depends(get_sirix),
):
    contacts_resource = get_json_store(sirix)
    result = await contacts_resource.find_by_key(contact_key, revision_timestamp or revision_id)
    return schemas.ContactWithKey(result, key=result["nodeKey"])


@app.get(
    "/contact/{contact_key}/history",
    response_model=Union[list[schemas.Revision], list[schemas.Contact]],
)
async def view_contact_history(
    contact_key: int,
    revision_id: Optional[int] = None,
    revision_timestamp: Optional[str] = None,
    embed: bool = False,
    sirix: Sirix = Depends(get_sirix),
):
    if embed:
        if revision_id is not None:
            resource = f'jn:open("contacts", "contacts", {revision_id})'
        elif revision_timestamp is not None:
            resource = (
                f'jn:open("contacts", "contacts", xs:dateTime({revision_timestamp}))'
            )
        else:
            resource = 'jn:doc("contacts", "contacts")'
        query = (
            # select the record from the resource
            f"let $node := sdb:select-item({resource}, {contact_key})"
            # iterate through every resource revision, returning every instance where the
            # record (hash) does not match the record in the previous revision
            " let $result := for $rev in jn:all-times($node) return"
            " if (not(exists(jn:previous($rev)))) then {$rev}"
            " else if (sdb:hash($rev) ne sdb:hash(jn:previous($rev))) then {$rev}"
            " else ()"
            # return the result
            " return $result"
        )
        results = await sirix.query(query)
        return [schemas.Contact(result, key=result["nodeKey"]) for result in results]
    else:
        contacts_resource = get_json_store(sirix)
        return await contacts_resource.history(
            contact_key, revision=revision_timestamp or revision_id
        )


@app.post("/contact/new", status_code=status.HTTP_204_NO_CONTENT)
async def new_contact(contact: schemas.Contact, sirix: Sirix = Depends(get_sirix)):
    contacts_resource = get_json_store(sirix)
    await contacts_resource.insert_one(contact.dict())


@app.patch("/contact/{contact_key}")
async def update_contact(
    contact_key: int, contact: schemas.Contact, sirix: Sirix = Depends(get_sirix)
):
    resource = get_json_resource(sirix)
    try:
        await resource.update(contact_key, contact.dict(), Insert.REPLACE)
    except SirixServerError:
        return Response(status_code=status.HTTP_410_GONE)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.delete("/contact/{contact_key}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_contact(contact_key: int, sirix: Sirix = Depends(get_sirix)):
    resource = get_json_resource(sirix)
    try:
        await resource.delete(contact_key)
    except SirixServerError:
        return Response(status_code=status.HTTP_410_GONE)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
