"""Strict request schemas for producer ingestion."""
from __future__ import annotations

import json
import re
import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

EVENT_TYPE = re.compile(r"^[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*$")


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Subject(StrictModel):
    type: Annotated[str, Field(min_length=1, max_length=100)]
    id: Annotated[str, Field(min_length=1, max_length=100)]


class Recipient(StrictModel):
    user_id: uuid.UUID
    channels: list[Literal["email", "in_app"]]

    @field_validator("channels")
    @classmethod
    def unique_nonempty_channels(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("must contain at least one channel")
        if len(value) != len(set(value)):
            raise ValueError("must not contain duplicate channels")
        return value


class IngestEvent(StrictModel):
    event_type: Annotated[str, Field(min_length=1, max_length=100)]
    subject: Subject
    payload: dict
    recipients: Annotated[list[Recipient], Field(min_length=1, max_length=100)]
    metadata: dict = Field(default_factory=dict)

    @field_validator("event_type")
    @classmethod
    def valid_event_type(cls, value: str) -> str:
        if not EVENT_TYPE.fullmatch(value):
            raise ValueError("must be an identifier containing only letters, numbers, dot, underscore, or dash")
        return value

    @field_validator("payload")
    @classmethod
    def payload_size(cls, value: dict) -> dict:
        if len(json.dumps(value, separators=(",", ":")).encode()) > 64 * 1024:
            raise ValueError("must not exceed 64 KiB")
        return value

    @field_validator("metadata")
    @classmethod
    def metadata_size(cls, value: dict) -> dict:
        if len(json.dumps(value, separators=(",", ":")).encode()) > 8 * 1024:
            raise ValueError("must not exceed 8 KiB")
        return value

    @model_validator(mode="after")
    def distinct_recipients(self) -> "IngestEvent":
        ids = [recipient.user_id for recipient in self.recipients]
        if len(ids) != len(set(ids)):
            raise ValueError("recipients must have distinct user_id values")
        return self
