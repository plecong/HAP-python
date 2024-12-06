from dataclasses import dataclass
from uuid import UUID

@dataclass
class HAPConnection:
    shared_key: bytes
    client_address: str
    client_uuid: UUID