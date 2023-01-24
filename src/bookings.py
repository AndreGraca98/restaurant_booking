import datetime
import logging
import sqlite3
from typing import List

import pandas as pd

from .db import Table
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
    tables_numbers: List[int],
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
            >= bookings_df.reservation_datetime.map(
                lambda x: get_reservation_time_limit(x, -time_limit + 1)
            )
        )
        & (
            # give some time for next client to arrive
            reservation_datetime
            < bookings_df.reservation_datetime.map(
                lambda x: get_reservation_time_limit(x, abs(time_limit - 1))
            )
        )
    ]

    return set(tables_numbers) - set(busy_df.table_number.values)


def is_table_available(
    tables_numbers: List[int],
    table_number: int,
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

    available_tables_numbers = get_available_tables(
        tables_numbers, bookings_df, reservation_datetime
    )

    bookingsLogger.debug(f"Available tables: {available_tables_numbers}")

    if table_number not in available_tables_numbers:
        return False
    return True


class Bookings:
    """Bookings class for managing client reservations"""

    def __init__(self, conn: sqlite3.Connection):
        self.bookings_table = Table("bookings", conn)
        self.tables_numbers = Table("tables", conn).get_df().table_number.values

        bookingsLogger.debug(f"Tables ids: {self.tables_numbers}")

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

        # Invalid table_id
        if table_number not in self.tables_numbers:
            bookingsLogger.warn(f"Table {table_number} does not exist.")
            return self

        # validate table is available at reservation_datetime
        df = self.bookings_table.get_df()

        bookingsLogger.debug(
            f"table_number={table_number} ; df.empty={df.empty} ; available={is_table_available(self.tables_numbers, table_number, df, reservation_datetime)}"
        )

        if df.empty or is_table_available(
            self.tables_numbers, table_number, df, reservation_datetime
        ):
            self.bookings_table.add(
                client_name=client_name,
                client_contact=client_contact,
                reservation_datetime=reservation_datetime,
                table_number=table_number,
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

        df = self.bookings_table.get_df()

        # Get client booking
        client_booking = df[
            (df.booking_id == booking_id)
            | ((df.client_name == client_name) & (df.client_contact == client_contact))
        ]

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
            tables_numbers=self.tables_numbers,
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
            tables_numbers=self.tables_numbers,
            bookings_df=self.bookings_table.get_df(),
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
    bookings = Bookings(conn)
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
    table_numbers = [1, 2, 3, 4, 11, 2, 3]

    for client_name, client_contact, reservation_datetime, table_number in zip(
        client_names, client_contacts, reservation_datetimes, table_numbers
    ):
        bookings.add(
            client_name=client_name,
            client_contact=client_contact,
            reservation_datetime=reservation_datetime,
            table_number=table_number,
        )

    bookingsLogger.debug(repr(bookings))


# ENDFILE
