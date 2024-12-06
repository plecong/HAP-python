import asyncio
import logging
from enum import Enum
from typing import List, Optional

from pyhap.hap_connection import HAPConnection
from pyhap.util import get_local_address

from .datastream_connection import (
    DatastreamConnection,
    DatastreamSession,
    MessageHandler,
)
from .hds_types import HDSFrame

logger = logging.getLogger(__name__)


class ServerState(Enum):
    UNINITIALIZED = 0
    LISTENING = 1
    CLOSING = 2
    CLOSED = 3


class DatastreamServer:
    HDS_CONNECTION_TIMEOUT = 10

    def __init__(
        self,
        message_callback: MessageHandler = None,
        host: str = None,
        port: int = None,
    ):
        self.state = ServerState.UNINITIALIZED
        self.host: str = host or get_local_address()
        self.port: int = port
        self.sessions: List[DatastreamSession] = []
        self.connections: List[DatastreamConnection] = []

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._server: asyncio.Server = None
        self._message_callback = message_callback

    async def prepare_session(
        self,
        connection: HAPConnection,
        controller_key_salt: bytes,
    ) -> DatastreamSession:
        """
        Prepare for an incoming HDS connection. This method will start the server if it
        is not already running, and return a session object. Called by DatastreamManagement
        when a new HDS connection is requested.
        """

        logger.debug(
            "Preparing for incoming HDS connection from %s",
            connection.client_address,
        )
        session = DatastreamSession(
            connection=connection, controller_key_salt=controller_key_salt
        )
        self.sessions.append(session)

        # Capture the running loop
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        # Set a timeout for the connection to be opened
        session.connection_task = self._loop.call_later(
            self.HDS_CONNECTION_TIMEOUT, lambda: self._timeout_session(session)
        )

        # Start the server if it is not already running
        try:
            session.port = await self.async_start()
        except Exception as e:
            logger.error("Failed to start HDS server: %s", e)
            self.sessions.remove(session)
            return None

        return session


    async def async_start(self) -> int:
        """
        Start the HDS server. If the server is already running, return the port.
        """

        match self.state:
            case ServerState.UNINITIALIZED:
                try:
                    logger.debug("Starting HDS server on %s", self.host)
                    self._server = await self._loop.create_server(
                        self._create_connection,
                        host=self.host,
                        port=self.port,
                    )
                    self.state = ServerState.LISTENING
                    sockname = self._server.sockets[0].getsockname()
                    logger.debug("HDS server started on %s", sockname)
                    return sockname[1]
                except Exception as e:
                    logger.error("Failed to start HDS server: %s", e)
                    self.state = ServerState.CLOSING

            case ServerState.LISTENING:
                sockname = self._server.sockets[0].getsockname()
                logger.debug("HDS server is already running on %s", sockname)
                return sockname[1]

            case ServerState.CLOSING:
                pass

    def _create_connection(self):
        """
        Create a new connection and add it to the list of connections.
        """

        connection = DatastreamConnection(self, self._message_callback)
        connection.add_close_handler(lambda: self._connection_closed(connection))
        self.connections.append(connection)
        return connection

    def identify_session(self, frame: HDSFrame):
        """
        Identify the session that the frame belongs to. If the frame can be decrypted
        by any of the sessions, return the session. Otherwise, return None.
        """
        identified_session = next(
            (session for session in self.sessions if session.crypto.decrypt(frame)),
            None,
        )

        if identified_session is not None:
            # remove the session from the list
            self.sessions.remove(identified_session)

            # cancel and remove the connection task
            identified_session.connection_task.cancel()
            identified_session.connection_task = None

        return identified_session

    def _timeout_session(self, session: DatastreamSession):
        """
        Callback for when a session times out. Remove the session from the list
        and check if the server can be closed.
        """
        logger.debug(
            "Prepared HDS session timed out since no connection was opened for 10 seconds (%s)",
            session.connection.client_uuid,
        )

        self.sessions.remove(session)
        self._check_closeable()    

    def _connection_closed(self, connection: DatastreamConnection):
        """
        Callback for when a connection is closed. Remove the connection from the list
        and check if the server can be closed.
        """
        self.connections.remove(connection)
        self._check_closeable()

    def _check_closeable(self):
        """
        Check if the server can be closed. If there are no active sessions or connections,
        and the server is not already closing, close it.
        """
        if (
            not self.sessions
            and not self.connections
            and self.state != ServerState.CLOSING
        ):
            self._state = ServerState.CLOSING
            self.close()

    def close(self):
        """
        Close the server and all connections.
        """
        if self._server is not None:
            self._server.close_clients()
            self._server.close()
            self._server = None
            self.state = ServerState.CLOSED
