from pydantic import BaseModel


class QueryTerm(BaseModel):
    # the term to match against
    term: str
    # whether to search for the lack of this term, instead of its presence
    invert: bool = False
    # which field in the record to match against
    field: str


class Contact(BaseModel):
    name: str
    phone: str
    email: str
    address: str


class ContactWithKey(Contact):
    key: int

    class Config:
        orm_mode = True


class Revision(BaseModel):
    """
    This is the form of pysirix.types.SubtreeRevision
    """

    revisionTimestamp: str
    revisionNumber: int
