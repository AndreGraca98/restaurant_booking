import datetime
import logging
import sqlite3
from typing import List, Tuple, Union

import pandas as pd
from dateutil import parser

from .clients import Clients
from .db import Table, all_valid_types, is_int, is_str, is_valid_type
from .log import add_console_handler
from .rest_tables import RestaurantTables

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


def normalize_datetime(reservation_datetime: str) -> str:
    return parser.parse(reservation_datetime).strftime("%Y-%m-%d %H:%M:%S")


class Bookings(Table):
    """Bookings class for managing client reservations"""

    def __init__(self, conn: sqlite3.Connection):
        super().__init__("bookings", conn, order_by="reservation_dt")

        self.restaurant_tables = RestaurantTables(conn)
        self.clients = Clients(conn)

    def _normalize(
        self,
        reservation_datetime: str,
        table_number: int,
        client_name: str = None,
        client_contact: str = None,
    ) -> Tuple[str, int, Union[str, None], Union[str, None]]:
        """Normalize booking data

        Args:
            reservation_datetime (str): Reservation datetime
            table_number (int): Table number
            client_name (str, optional): Client name. Defaults to None.
            client_contact (str, optional): Client contact. Defaults to None.

        Returns:
            Tuple[str, int, Union[str, None], Union[str, None]]: Normalized data
        """

        n_dt = normalize_datetime(reservation_datetime)
        n_t_num = self.restaurant_tables._normalize(table_number)
        n_c_n, n_c_c = self.clients._normalize(client_name, client_contact)

        return n_dt, n_t_num, n_c_n, n_c_c

    def add(
        self,
        reservation_datetime: str,
        table_number: int,
        client_name: str = None,
        client_contact: str = None,
    ):
        """Add a new booking. The client_name and client_contact are not required but are recommended.

        Args:
            reservation_datetime (str): Reservation datetime
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
            bookingsLogger.error(
                f"Table {table_number} does not exist in {self.restaurant_tables.available_numbers}."
            )
            return self

        # Normalize inputs
        r_dt, t_num, c_n, c_c = self._normalize(
            reservation_datetime, table_number, client_name, client_contact
        )

        # Get client_id and try to add client if not exists
        if self.clients.exists(c_n, c_c):
            c_id = self.clients.get_id(c_n, c_c)
        else:
            c_id = self.clients.add(c_n, c_c).get_id(c_n, c_c)
            if c_id is None:
                bookingsLogger.error(f"Failed to add client '{c_n}'")
                return self

        # Get table_id
        t_id = self.restaurant_tables.get_id(t_num)

        # validate table is available at reservation_datetime
        df = self.as_df

        t_available = is_table_available(
            self.restaurant_tables.available_ids, t_id, df, r_dt
        )

        bookingsLogger.debug(
            f"table_number={t_num} ; df.empty={df.empty} ; available={t_available}"
        )

        if df.empty or t_available:
            super().add(
                client_id=c_id,
                reservation_dt=r_dt,
                table_id=t_id,
            )

            bookingsLogger.info(f"Added booking for {c_n} at {r_dt} for table {t_num}")
            return self

        bookingsLogger.warn(
            f"Table {t_num} is not available at {reservation_datetime}."
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
            bookingsLogger.error(
                "Must provide booking_id or (client_name and client_contact)"
            )
            return self

        if not (reservation_datetime or table_number):
            bookingsLogger.warn("Must provide reservation_datetime or table_id")
            return self

        c_n, c_c = self.clients._normalize(client_name, client_contact)
        t_num = (
            self.restaurant_tables._normalize(table_number) if table_number else None
        )
        r_dt = (
            normalize_datetime(reservation_datetime) if reservation_datetime else None
        )

        c_id = self.clients.get_id(c_n, c_c)
        t_id = self.restaurant_tables.get_id(t_num)

        df = self.as_df

        # Get client booking
        client_booking = df[(df.booking_id == booking_id) | (df.client_id == c_id)]

        if client_booking.empty:
            bookingsLogger.warn(
                f"Booking {booking_id} or ({c_n}, {c_c}) does not exist."
            )
            return self

        # if values not provided, use values from client_booking
        booking_id = booking_id or int(client_booking.booking_id.values[0])
        c_id = c_id or str(client_booking.client_id.values[0])
        r_dt = r_dt or client_booking.reservation_dt.values[0]
        t_id = t_id or int(client_booking.table_id.values[0])

        # Checks new availability
        if not is_table_available(
            tables_ids=self.restaurant_tables.available_ids,
            table_id=t_id,
            bookings_df=df,
            reservation_datetime=r_dt,
        ):
            bookingsLogger.warn(f"Table {table_number} is not available at {r_dt}")
            return self

        # Update reservation details
        self.update_multiple(
            f"booking_id={booking_id} AND client_id={c_id}",
            cols=["reservation_dt", "table_id"],
            values=[r_dt, t_id],
        )

        bookingsLogger.info(f"Updated booking {booking_id}.")

        return self

    def delete(
        self,
        booking_id: int = None,
        client_name: str = None,
        client_contact: str = None,
        reservation_datetime: str = None,
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

        if booking_id is not None:
            super().delete(f"booking_id={booking_id}")
            bookingsLogger.info(f"Deleted booking {booking_id}.")
            return self

        c_n, c_c = self.clients._normalize(client_name, client_contact)
        r_dt = (
            normalize_datetime(reservation_datetime) if reservation_datetime else None
        )

        c_id = self.clients.get_id(c_n, c_c)

        if c_id is None:
            bookingsLogger.warn(f"Booking for {client_name} does not exist.")
            return self

        if r_dt is not None:
            super().delete(f"client_id={c_id} AND reservation_dt='{r_dt}'")
            bookingsLogger.info(f"Deleted booking for {client_name} at {r_dt}")
            return self

        super().delete(f"client_id={c_id}")
        bookingsLogger.info(f"Deleted all bookings for {client_name}")
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

        r_dt = normalize_datetime(reservation_datetime)

        available_tables = get_available_tables(
            tables_ids=self.restaurant_tables.available_ids,
            bookings_df=self.as_df,
            reservation_datetime=r_dt,
        )
        bookingsLogger.info(
            f"Available tables at {reservation_datetime} : {available_tables}"
        )
        return self


def create_dummy_bookings(conn: sqlite3.Connection):
    # client_names = [None, "John", "Mary", "Jack", None, "Peter"]
    # client_contacts = [
    #     "+351 123 456 789",
    #     None,
    #     "+351 987 654 321",
    #     "+351 111 222 333",
    #     None,
    #     "+351 444 555 666",
    # ]
    # reservation_datetimes = [
    #     "2023-02-01 12:00:00",
    #     "2023-02-01 13:00:00",
    #     "2023-02-01 14:00:00",
    #     "2023-02-02 11:00:00",
    #     "2023-02-02 14:00:00",
    #     "2023-02-01 15:00:00",
    # ]
    # table_numbers = [11, 2, 3, 4, 11, 1]

    # bookings = Bookings(conn)
    # for client_name, client_contact, reservation_datetime, table_number in zip(
    #     client_names, client_contacts, reservation_datetimes, table_numbers
    # ):
    #     bookings.add(
    #         reservation_datetime=reservation_datetime,
    #         table_number=table_number,
    #         client_name=client_name,
    #         client_contact=client_contact,
    #     )

    # bookingsLogger.debug(repr(bookings))

    bookings = Bookings(conn)
    bookings.add("2023/02/01 12:00", 11, "John", "+351 123 456 789")
    bookings.add("2023/02/01 12:00", 2, "andré graça")

    bookings.show()

    bookings.update(booking_id=1, table_number=2)
    bookings.update(client_name="andré graça", table_number=3)

    bookings.show()
    bookings.show_available_tables("2023/02/01 12:00")


# ENDFILE
