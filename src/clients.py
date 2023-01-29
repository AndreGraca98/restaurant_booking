import logging
import sqlite3
from typing import List, Tuple, Union

from .db import Table, is_str
from .log import add_console_handler

clientsLogger = logging.getLogger(__name__)
add_console_handler(clientsLogger)


class Clients(Table):
    """Clients class for managing clients"""

    def __init__(
        self,
        conn: sqlite3.Connection,
        order_by: Union[str, List[str]] = None,
    ):
        super().__init__("clients", conn, order_by)

    def _normalize(self, client_name: str, client_contact: str) -> Tuple[str, str]:
        c_name = client_name.title() if client_name else None
        c_contact = client_contact.replace(" ", "") if client_contact else None
        return c_name, c_contact

    def add(self, client_name: str, client_contact: str):
        """Add a new client

        Args:
            client_name (str): Client name
            client_contact (str): Client contact

        Returns:
            _type_: Self

        Example:
            >>> Clients(db.conn).add("John", "+351 111 222 333")
        """
        if not (is_str(client_name) and is_str(client_contact)):
            clientsLogger.error(
                f"Failed to create client! client_name and client_contact must be a str: type(client_name)={type(client_name).__name__} ; type(client_contact)={type(client_contact).__name__}"
            )
            return self

        c_name, c_contact = self._normalize(client_name, client_contact)

        return super().add(
            client_name=c_name,
            client_contact=c_contact,
        )

    def update(self, client_id: int, col: str, value: str):
        """Update a client

        Args:
            client_id (int): Client id
            col (str): Column name
            value (str): Column value

        Returns:
            _type_: Self

        Example:
            >>> Clients(db.conn).update(1, "client_name", "John")
            >>> Clients(db.conn).update(1, "client_contact", "+351 111 222 333")
        """
        if not (is_str(col) and is_str(value)):
            clientsLogger.error(
                f"Failed to update client! col and value must be a str: type(col)={type(col).__name__} ; type(value)={type(value).__name__}"
            )
            return self

        return super().update(
            f"client_id={client_id}",
            col,
            value,
        )

    def delete(self, client_id: int):
        """Remove client by id

        Args:
            client_id (int): Client id

        Returns:
            _type_: Self

        Example:
            >>> Clients(db.conn).remove(1)
        """
        if client_id is None or client_id == []:
            clientsLogger.error(
                f"client_id must be a int: type(client_id)={type(client_id)}"
            )
            return self

        return super().clients_table.delete(f"client_id={client_id}")

    def get_id(
        self, client_name: str = None, client_contact: str = None
    ) -> Union[int, None]:
        """Get client by name or contact

        Args:
            client_name (str): Client name
            client_contact (str): Client contact

        Returns:
            _type_: Self

        Example:
            >>> Clients(db.conn).get_id(client_name="John")
            >>> Clients(db.conn).get_id(client_contact="+351 111 222 333")
            >>> Clients(db.conn).get_id(client_name="John", client_contact="+351 111 222 333")
        """

        c_name, c_contact = self._normalize(client_name, client_contact)

        df = self.as_df
        query = df[(df.client_name == c_name) | (df.client_contact == c_contact)]

        # First row, client_id column
        if not query.empty:
            return int(query.client_id.values[0])

        clientsLogger.warn(f"Client not found: '{c_name}' , '{c_contact}'")
        return None

    def exists(self, client_name: str = None, client_contact: str = None) -> bool:
        """Check if client exists

        Args:
            client_name (str): Client name
            client_contact (str): Client contact

        Returns:
            bool: True if client exists, False otherwise

        Example:
            >>> Clients(db.conn).exists(client_name="John")
            >>> Clients(db.conn).exists(client_contact="+351 111 222 333")
            >>> Clients(db.conn).exists(client_name="John", client_contact="+351 111 222 333")
        """
        return True if self.get_id(client_name, client_contact) is not None else False


def create_client(conn):
    """Create clients table

    Args:
        conn (sqlite3.Connection): Database connection

    Returns:
        _type_: Self

    Example:
        >>> create_client(db.conn)
    """
    clientsLogger.debug("Creating clients")

    clients = Clients(conn)
    clients.add("andré graça", "+351 967 51 53 55")
    clients.add("joão silva", "+1 234 567 890")
    clients.add("joão silva", "+0 987 654 321")

    clients.show()


# ENDFILE
