from datetime import datetime

from pydantic import BaseModel, Field


class CorpusCreateRequest(BaseModel):
    name: str = Field(min_length=1)
    read_group: str = Field(min_length=1)
    write_group: str = Field(min_length=1)
    owner_group: str = Field(min_length=1)


class CorpusResponse(BaseModel):
    id: str
    name: str
    read_group: str
    write_group: str
    owner_group: str
    created_by: str
    created_at: datetime
    can_read: bool
    can_write: bool
    is_owner: bool
