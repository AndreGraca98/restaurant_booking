import datetime
import logging
import sqlite3
from typing import List, Tuple

import pandas as pd

from .db import Table, all_valid_types, is_int, is_str, is_valid_type
from .log import add_console_handler

bookingsLogger = logging.getLogger(__name__)
add_console_handler(bookingsLogger)


def get_reservation_time_limit(reservation_datetime: str, time_limit: int = 60) -> str:
    """Get reservation time according to time_limit

    Args:
        reservation_datetime (str): Start time of reservation in isoformat. YYYY-MM-DD HH:MM:SS
        time_limit (int, optional): Time in minutes to add to reservation_datetime. Can be a negative number. Defaults to 60.

    Returns:
        str: Time of reservation in isoformat. YYYY-MM-DD HH:MM:SS

    Example:
        >>> get_reservation_time_limit("2023-02-01 12:00:00", 60) # "2023-02-01 13:00:00"
        >>> get_reservation_time_limit("2023-02-01 12:00:00", -60) # "2023-02-01 11:00:00"
    """

    return (
        datetime.datetime.fromisoformat(reservation_datetime)
        + datetime.timedelta(minutes=time_limit)
    ).isoformat(sep=" ")


def get_available_tables(
    tables_ids: List[int],
    bookings_df: pd.DataFrame,
    reservation_datetime: str,
    time_limit: int = 60,
) -> pd.DataFrame:
    """Get available tables at reservation_datetime

    Args:
        tables_numbers (List[int]): List of tables numbers
        bookings_df (pd.DataFrame): Query result from bookings
        reservation_datetime (str): Reservation datetime in iso format YYYY-MM-DD HH:MM:SS
        time_limit (int, optional): Time in minutes to add to reservation_datetime. Can be a negative number. Defaults to 60.

    Returns:
        pd.DataFrame: Available tables at reservation_datetime

    Example:
        >>> get_available_tables([1, 2, 3], bookings_df, "2023-02-01 12:00:00", 60)
    """

    time_limit = abs(time_limit)

    busy_df = bookings_df[
        (
            # give some time for current client to leave
            reservation_datetime
            >= bookings_df.reservation_dt.map(
                lambda x: get_reservation_time_limit(x, -time_limit + 1)
            )
        )
        & (
            # give some time for next client to arrive
            reservation_datetime
            < bookings_df.reservation_dt.map(
                lambda x: get_reservation_time_limit(x, abs(time_limit - 1))
            )
        )
    ]

    return set(tables_ids) - set(busy_df.table_id.values)


def is_table_available(
    tables_ids: List[int],
    table_id: int,
    bookings_df: pd.DataFrame,
    reservation_datetime: str,
) -> bool:
    """Check if table is available at reservation_datetime

    Args:
        tables_numbers (List[int]): List of tables numbers
        table_number (int): Table number
        bookings_df (pd.DataFrame): Query result from bookings
        reservation_datetime (str): Reservation datetime in isoformat. YYYY-MM-DD HH:MM:SS

    Returns:
        bool: True if table is available, False otherwise

    Example:
        >>> is_table_available(tables_numbers, 1, df, "2023-02-01 12:00:00")
    """

    available_tables_ids = get_available_tables(
        tables_ids, bookings_df, reservation_datetime
    )

    bookingsLogger.debug(f"Available table ids: {available_tables_ids}")

    if table_id not in available_tables_ids:
        return False
    return True


class Clients:
    """Clients class for managing clients"""

    def __init__(self, conn: sqlite3.Connection):
        self.clients_table = Table("clients", conn)

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
            bookingsLogger.error(
                f"client_name and client_contact must be a str: type(client_name)={type(client_name)} ; type(client_contact)={type(client_contact)}"
            )
            return self

        c_name, c_contact = self._normalize(client_name, client_contact)

        self.clients_table.add(
            client_name=c_name,
            client_contact=c_contact,
        )

        return self

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
            bookingsLogger.error(
                f"client_id must be a int: type(client_id)={type(client_id)}"
            )
            return self

        self.clients_table.delete(f"client_id={client_id}")

        return self

    def get_id(self, client_name: str = None, client_contact: str = None):
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

        stmt = "SELECT client_id FROM clients WHERE "

        if c_name is not None and c_contact is not None:
            stmt += f"client_name='{c_name}' AND client_contact='{c_contact}';"
        elif c_name is not None:
            stmt += f"client_name='{c_name}';"
        elif c_contact is not None:
            stmt += f"client_contact='{c_contact}';"
        else:
            bookingsLogger.error("client_name or client_contact must be provided")
            return None

        query = self.clients_table.select(stmt)

        # First row, client_id column
        if query:
            return int(query[0][0])

        bookingsLogger.warn(f"Client not found: '{c_name}' , '{c_contact}'")
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
        return True if self.get_id(client_name, client_contact) else False


class Tables:
    """Tables class for managing restaurant tables"""

    def __init__(self, conn: sqlite3.Connection):
        self.tables_table = Table("tables", conn)

        self.available_numbers = self.tables_table.as_df.table_number.tolist()
        self.available_ids = self.tables_table.as_df.table_id.tolist()

    def _normalize(self, table_number: int) -> int:
        return int(table_number)

    def add(self, table_number: int, capacity: int = 4):
        """Add a new table

        Args:
            table_number (int): Table number

        Returns:
            _type_: Self

        Example:
            >>> Tables(db.conn).add(1)
        """
        if not is_int(table_number):
            bookingsLogger.error(
                f"table_number must be a int: type(table_number)={type(table_number)}"
            )
            return self

        self.tables_table.add(table_number=table_number, capacity=capacity)

        return self

    def delete(self, table_id: int):
        """Remove table by id

        Args:
            table_id (int): Table id

        Returns:
            _type_: Self

        Example:
            >>> Tables(db.conn).remove(1)
        """
        if table_id is None or table_id == []:
            bookingsLogger.error(
                f"table_id must be a int: type(table_id)={type(table_id)}"
            )
            return self

        self.tables_table.delete(f"table_id={table_id}")

        return self

    def get_id(self, table_number: int):
        """Get table by number

        Args:
            table_number (int): Table number

        Returns:
            _type_: Self

        Example:
            >>> Tables(db.conn).get_id(1)
        """
        if not is_int(table_number):
            bookingsLogger.error(
                f"table_number must be a int: type(table_number)={type(table_number)}"
            )
            return None

        # stmt = f"SELECT table_id FROM 'tables' WHERE table_number = {table_number}"
        # query = self.tables_table.select(stmt)

        df = self.tables_table.as_df
        query = df[df.table_number == table_number]

        # First row, table_id column
        if not query.empty:
            return int(query.table_id.values[0])

        bookingsLogger.warn(f"Table not found: {table_number}")
        return None

    def exists(self, table_number: int) -> bool:
        """Check if table exists

        Args:
            table_number (int): Table number

        Returns:
            bool: True if table exists, False otherwise

        Example:
            >>> Tables(db.conn).exists(1)
        """
        return True if self.get_id(table_number) else False


class Bookings:
    """Bookings class for managing client reservations"""

    def __init__(self, conn: sqlite3.Connection):
        self.bookings_table = Table("bookings", conn)

        self.restaurant_tables = Tables(conn)
        self.clients = Clients(conn)

        # bookingsLogger.debug(f"Tables ids: {self.tables_ids}")

    def add(
        self,
        reservation_datetime: str,
        table_number: int,
        client_name: str = None,
        client_contact: str = None,
    ):
        """Add a new booking. The client_name and client_contact are not required but are recommended.

        Args:
            reservation_datetime (str): Reservation datetime in isoformat. YYYY-MM-DD HH:MM:SS
            table_number (int): Table number
            client_name (str): Client name
            client_contact (str): Client contact

        Returns:
            _type_: Self

        Example:
            >>> Bookings(db).add("2023-02-01 12:00:00", 1, client_name="John", client_contact="+351 111 222 333"
            >>> Bookings(db).add("2023-02-01 12:00:00", 1)
        """
        # Invalid table_number
        if not self.restaurant_tables.exists(table_number):
            bookingsLogger.warn(
                f"Table {table_number} does not exist in {self.restaurant_tables.available_numbers}."
            )
            return self

        # Get table_id
        t_id = self.restaurant_tables.get_id(table_number)

        # Get client_id
        c_id = self.clients.get_id(client_name, client_contact)
        if not c_id:
            # Add client if not exists
            c_id = self.clients.add(client_name, client_contact).get_id(
                client_name, client_contact
            )

        # validate table is available at reservation_datetime
        df = self.bookings_table.as_df

        t_available = is_table_available(
            self.restaurant_tables.available_ids, t_id, df, reservation_datetime
        )

        bookingsLogger.debug(
            f"table_number={table_number} ; df.empty={df.empty} ; available={t_available}"
        )

        if df.empty or t_available:
            self.bookings_table.add(
                client_id=c_id,
                reservation_dt=reservation_datetime,
                table_id=t_id,
            )

            bookingsLogger.info(
                f"Added booking for {client_name} at {reservation_datetime} for table {table_number}"
            )
            return self

        bookingsLogger.warn(
            f"Table {table_number} is not available at {reservation_datetime}."
        )
        return self

    def update(
        self,
        booking_id: int = None,
        client_name: str = None,
        client_contact: str = None,
        reservation_datetime: str = None,
        table_number: int = None,
    ):
        """Update a booking

        Args:
            booking_id (int, optional): Booking id. Defaults to None.
            client_name (str, optional): Client name . Defaults to None.
            client_contact (str, optional): Client contact . Defaults to None.
            reservation_datetime (str, optional): Reservation datetime in iso format . Defaults to None.
            table_id (int, optional): Table id. Defaults to None.

        Returns:
            _type_: Self

        Example:
            >>> Bookings(db).update(booking_id=1, reservation_datetime="2023-02-01 12:00:00", table_id=1)
            >>> Bookings(db).update(client_name="John", client_contact="+351 123 456 789", reservation_datetime="2023-02-01 12:00:00", table_id=1)
            >>> Bookings(db).update(booking_id=1, table_id=1)
        """
        if not booking_id and not (client_name and client_contact):
            bookingsLogger.warn(
                "Must provide booking_id or (client_name and client_contact)"
            )
            return self

        if not (reservation_datetime or table_number):
            bookingsLogger.warn("Must provide reservation_datetime or table_id")
            return self

        df = self.bookings_table.as_df

        # Get client booking
        client_booking = df[
            (df.booking_id == booking_id)
            | ((df.client_name == client_name) & (df.client_contact == client_contact))
        ]

        if client_booking.empty:
            bookingsLogger.warn(
                f"Booking {booking_id} or ({client_name}, {client_contact}) does not exist."
            )
            return self

        # if values not provided, use values from client_booking
        booking_id = booking_id or int(client_booking.booking_id.values[0])
        client_name = client_name or client_booking.client_name.values[0]
        client_contact = client_contact or client_booking.client_contact.values[0]
        table_number = table_number or int(client_booking.table_id.values[0])
        reservation_datetime = (
            reservation_datetime or client_booking.reservation_datetime.values[0]
        )

        # Checks new availability
        if not is_table_available(
            tables_numbers=self.tables_ids,
            table_number=table_number,
            bookings_df=df,
            reservation_datetime=reservation_datetime,
        ):
            bookingsLogger.warn(
                f"Table {table_number} is not available at {reservation_datetime}"
            )
            return self

        # Update reservation details
        self.bookings_table.update_multiple(
            f"booking_id={booking_id} AND client_name='{client_name}' AND client_contact='{client_contact}'",
            cols=["reservation_datetime", "table_number"],
            values=[reservation_datetime, table_number],
        )

        bookingsLogger.info(f"Updated booking {booking_id}.")

        return self

    def delete(
        self,
        booking_id: int = None,
        client_name: str = None,
        client_contact: str = None,
    ):
        """Delete a booking

        Args:
            booking_id (int, optional): Booking id . Defaults to None.
            client_name (str, optional): Client name . Defaults to None.
            client_contact (str, optional): Client contact. Defaults to None.

        Returns:
            _type_: Self

        Example:
            >>> Bookings(db).delete(booking_id=1)
            >>> Bookings(db).delete(client_name="John", client_contact="+351 123 456 789")
        """
        if not booking_id and not (client_name and client_contact):
            bookingsLogger.warn(
                "Must provide booking_id or (client_name and client_contact)"
            )
            return self

        if booking_id:
            self.bookings_table.delete(f"booking_id={booking_id}")
            bookingsLogger.info(f"Deleted booking {booking_id}.")
            return self

        self.bookings_table.delete(
            f"client_name='{client_name}' and client_contact='{client_contact}'"
        )
        bookingsLogger.info(f"Deleted booking for {client_name}")
        return self

    def show(self):
        """Show all bookings

        Returns:
            _type_: Self

        Example:
            >>> Bookings(conn).show()
        """
        self.bookings_table.show()
        return self

    def show_available_tables(self, reservation_datetime: str, time_limit: int = 60):
        """Show available tables

        Args:
            reservation_datetime (str): Reservation datetime in iso format YYYY-MM-DD HH:MM:SS
            time_limit (int, optional): Time limit in minutes. Defaults to 60.

        Returns:
            _type_: _description_

        Example:
            >>> Bookings(db).show_available_tables(reservation_datetime="2023-02-01 12:00:00")
        """

        available_tables = get_available_tables(
            tables_numbers=self.tables_ids,
            bookings_df=self.bookings_table.as_df,
            reservation_datetime=reservation_datetime,
            time_limit=time_limit,
        )
        bookingsLogger.info(
            f"Available tables at {reservation_datetime} : {available_tables}"
        )
        return self

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({str(self.bookings_table)})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self.bookings_table)})"


def create_dummy_bookings(conn: sqlite3.Connection):
    client_names = [None, "John", "Mary", "Jack", None, "Peter"]
    client_contacts = [
        "+351 123 456 789",
        None,
        "+351 987 654 321",
        "+351 111 222 333",
        None,
        "+351 444 555 666",
    ]
    reservation_datetimes = [
        "2023-02-01 12:00:00",
        "2023-02-01 13:00:00",
        "2023-02-01 14:00:00",
        "2023-02-02 11:00:00",
        "2023-02-02 14:00:00",
        "2023-02-01 15:00:00",
    ]
    table_numbers = [11, 2, 3, 4, 11, 1]

    bookings = Bookings(conn)
    for client_name, client_contact, reservation_datetime, table_number in zip(
        client_names, client_contacts, reservation_datetimes, table_numbers
    ):
        bookings.add(
            reservation_datetime=reservation_datetime,
            table_number=table_number,
            client_name=client_name,
            client_contact=client_contact,
        )

    bookingsLogger.debug(repr(bookings))


# ENDFILE
