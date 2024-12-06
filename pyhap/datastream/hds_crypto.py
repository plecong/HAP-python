import os
import struct

from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305

from .hds_types import HDSFrame
from pyhap.hap_crypto import hap_hkdf

ACCESSORY_TO_CONTROLLER_INFO = b"HDS-Read-Encryption-Key"
CONTROLLER_TO_ACCESSORY_INFO = b"HDS-Write-Encryption-Key"


class HDSCrypto:
    def __init__(self, shared_key: bytes, controller_key_salt: bytes):
        self.accessory_key_salt = os.urandom(32)

        self._in_nonce = 0
        self._out_nonce = 0

        salt = controller_key_salt + self.accessory_key_salt
        self._in_cipher = ChaCha20Poly1305(
            hap_hkdf(shared_key, salt, CONTROLLER_TO_ACCESSORY_INFO)
        )
        self._out_cipher = ChaCha20Poly1305(
            hap_hkdf(shared_key, salt, ACCESSORY_TO_CONTROLLER_INFO)
        )

    def decrypt(self, frame: HDSFrame) -> bool:
        if frame.plaintext_payload is not None:
            return True
        try:
            frame.plaintext_payload = self._in_cipher.decrypt(
                nonce=struct.pack("<LQ", 0, self._in_nonce),
                data=frame.ciphered_payload + frame.auth_tag,
                associated_data=frame.header,
            )
            self._in_nonce += 1
            return True
        except Exception as e:
            return False

    def encrypt(self, frame: HDSFrame) -> HDSFrame:
        nonce = struct.pack("<LQ", 0, self._out_nonce)
        frame.ciphered_payload = self._out_cipher.encrypt(
            nonce=nonce,
            data=frame.plaintext_payload,
            associated_data=frame.header,
        )
        self._out_nonce += 1
        return frame
