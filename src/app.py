from datetime import datetime
from typing import Union, Optional

from fastapi import FastAPI, Depends, status, Response
from pysirix import Sirix, SirixServerError, Insert

from .depends import get_json_resource, get_sirix, get_json_store
from . import schemas


app = FastAPI()


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
async def new_contact(contact: schemas.Contact, sirix: Sirix = Depends(get_sirix)):
    """

    :param contact:
    :param sirix:
    :return:
    """
    contacts_resource = get_json_store(sirix)
    await contacts_resource.insert_one(contact.dict())


@app.post("/search", response_model=list[schemas.ContactWithKey])
async def search_contacts(
    query_terms: list[schemas.QueryTerm],
    revision_id: Optional[int] = None,
    revision_timestamp: Optional[str] = None,
    sirix: Sirix = Depends(get_sirix),
):
    """

    :param query_terms:
    :param revision_id:
    :param revision_timestamp:
    :param sirix:
    :return:
    """
    contacts_resource = get_json_store(sirix)
    results = await contacts_resource.find_all(
        {query_term.field: query_term.term for query_term in query_terms},
        revision=parse_revision(revision_id, revision_timestamp),
    )
    return [schemas.ContactWithKey(**result, key=result["nodeKey"]) for result in results]


@app.get("/contact/{contact_key}", response_model=schemas.Contact)
async def view_contact(
    contact_key: int,
    revision_id: Optional[int] = None,
    revision_timestamp: Optional[str] = None,
    sirix: Sirix = Depends(get_sirix),
):
    """

    :param contact_key:
    :param revision_id:
    :param revision_timestamp:
    :param sirix:
    :return:
    """
    contacts_resource = get_json_store(sirix)
    result = await contacts_resource.find_by_key(
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
    sirix: Sirix = Depends(get_sirix),
):
    """

    :param contact_key:
    :param revision_id:
    :param revision_timestamp:
    :param embed:
    :param sirix:
    :return:
    """
    contacts_resource = get_json_store(sirix)
    if embed:
        results = await contacts_resource.history_embed(
            contact_key, parse_revision(revision_id, revision_timestamp)
        )
        print(results)
        return [schemas.HistoricalContact(**result) for result in results]
    else:
        return await contacts_resource.history(
            contact_key, revision=parse_revision(revision_id, revision_timestamp)
        )


@app.patch("/contact/{contact_key}", status_code=status.HTTP_204_NO_CONTENT)
async def update_contact(
    contact_key: int, contact: schemas.Contact, sirix: Sirix = Depends(get_sirix)
):
    """

    :param contact_key:
    :param contact:
    :param sirix:
    :return:
    """
    contacts_resource = get_json_store(sirix)
    await contacts_resource.update_by_key(contact_key, contact.dict())


@app.delete("/contact/{contact_key}")
async def delete_contact(contact_key: int, sirix: Sirix = Depends(get_sirix)):
    """

    :param contact_key:
    :param sirix:
    :return:
    """
    resource = get_json_resource(sirix)
    try:
        await resource.delete(contact_key, None)
    except SirixServerError:
        return Response(status_code=status.HTTP_410_GONE)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
