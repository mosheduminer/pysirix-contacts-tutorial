from pydantic import BaseModel, root_validator
from typing import Optional


class QueryTerm(BaseModel):
    # the term to match against
    term: str
    # whether to match when the field string contains `term`, instead of looking for an exact match
    fuzzy: bool = False
    # which field in the record to match against
    field: str


class Contact(BaseModel):
    name: Optional[str]
    phone: Optional[str]
    email: Optional[str]
    address: Optional[str]

    @root_validator
    def check_not_empty(cls, fields):
        """
        At least 1 field must be truthy.
        """
        assert any(
            fields.values()
        ), "At least 1 field must not be None/null, and not empty"
        return fields


class ContactWithMeta(Contact):
    key: int
    hash: str


class Revision(BaseModel):
    """
    This schema is of the form of pysirix.types.SubtreeRevision
    """

    revisionTimestamp: str
    revisionNumber: int


class HistoricalContact(Revision):
    """
    This schema is of the form of pysirix.types.QueryResult
    """

    revision: Contact
