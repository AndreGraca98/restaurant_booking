import datetime
import logging

import pandas as pd

from .db import Database, Table
from .log import add_console_handler

restaurantLogger = logging.getLogger(__name__)
add_console_handler(restaurantLogger)


def create_menu(db: Database):
    menu = Table("menu", db.conn)
    menu.add_multiple(
        name=[
            "Chicken Burger",
            "Pork Burger",
            "Fries",
            "Soda",
            "Salad",
            "Steak",
            "Chicken",
            "Pizza",
        ],
        price=[500, 550, 200, 100, 300, 700, 600, 400],
    )

    restaurantLogger.info(repr(menu))


def create_tables(db: Database):
    tables = Table("tables", db.conn)
    tables.add_multiple(
        capacity=[4, 4, 2, 2],
    )

    restaurantLogger.info(repr(tables))


def create_bookings(db: Database):
    bookings = Table("bookings", db.conn)
    bookings.add_multiple(
        client_name=["John", "Mary", "Jack", "Peter"],
        client_contact=[
            "+351 123 456 789",
            "+351 987 654 321",
            "+351 111 222 333",
            "+351 444 555 666",
        ],
        reservation_datetime=[
            "2023-02-01 12:00:00",
            "2023-02-01 13:00:00",
            "2023-02-01 14:00:00",
            "2023-02-01 15:00:00",
        ],
        table_id=[1, 2, 3, 4],
    )

    restaurantLogger.info(repr(bookings))


def get_reservation_time_limit(
    reservation_datetime: str, minutes_limit: int = 60
) -> str:
    """Get reservation time according to minutes_limit

    Args:
        reservation_datetime (str): Start time of reservation in isoformat. YYYY-MM-DD HH:MM:SS
        minutes_limit (int, optional): Minutes to add to reservation_datetime. Can be a negative number. Defaults to 60.

    Returns:
        str: Time of reservation in isoformat. YYYY-MM-DD HH:MM:SS

    Example:
        >>> get_reservation_time_limit("2023-02-01 12:00:00", 60) # "2023-02-01 13:00:00"
        >>> get_reservation_time_limit("2023-02-01 12:00:00", -60) # "2023-02-01 11:00:00"
    """

    return (
        datetime.datetime.fromisoformat(reservation_datetime)
        + datetime.timedelta(minutes=minutes_limit)
    ).isoformat(sep=" ")


def is_table_available(
    df: pd.DataFrame, table_id: int, reservation_datetime: str
) -> bool:
    """Check if table is available at reservation_datetime

    Args:
        df (pd.DataFrame): Query result from bookings
        table_id (int): Table id
        reservation_datetime (str): Reservation datetime in isoformat. YYYY-MM-DD HH:MM:SS

    Returns:
        bool: True if table is available, False otherwise

    Example:
        >>> check_table_availability(df, 1, "2023-02-01 12:00:00")
    """
    available_df = df[
        (
            reservation_datetime
            <= df.reservation_datetime.map(
                lambda x: get_reservation_time_limit(
                    x, -60
                )  # To avoid conflict assume people eat in 60min
            )
        )
        | (
            reservation_datetime
            >= df.reservation_datetime.map(lambda x: get_reservation_time_limit(x, 60))
        )
    ]

    restaurantLogger.debug(f"Available tables: {available_df.table_id.values}")

    if table_id not in available_df.table_id.values:
        return False
    return True


class Bookings:
    def __init__(self, db: Database):
        self.db = db
        self.table = Table("bookings", self.db.conn)

    def add(
        self,
        client_name: str,
        client_contact: str,
        reservation_datetime: str,
        table_id: int,
    ):
        """Add a new booking. Table must be available from reservation_datetime to reservation_datetime + 60 minutes

        Args:
            client_name (str): Client name
            client_contact (str): Client contact
            reservation_datetime (str): Reservation datetime in isoformat. YYYY-MM-DD HH:MM:SS
            table_id (int): Table id

        Returns:
            _type_: Self
        """

        # validate table_id exists and is available at reservation_datetime
        tables = Table("bookings", self.db.conn)

        df = tables.query()

        if not is_table_available(df, table_id, reservation_datetime):
            restaurantLogger.warn(
                f"Table {table_id} is not available at {reservation_datetime}."
            )
            return self

        # available_tables = tables.select(
        #     f"""
        #     id IN (
        #         SELECT table_id
        #         FROM booking
        #         WHERE reservation_datetime BETWEEN '{reservation_datetime}' and '{get_reservation_time_limit(reservation_datetime)}'
        #     )
        #     """
        # )

        self.table.add(
            client_name=client_name,
            client_contact=client_contact,
            reservation_datetime=reservation_datetime,
            table_id=table_id,
        )

        restaurantLogger.info(
            f"Added booking for {client_name} at {reservation_datetime} for table {table_id}"
        )

        return self

    def update(
        self,
        booking_id: int = None,
        client_name: str = None,
        client_contact: str = None,
        reservation_datetime: str = None,
        table_id: int = None,
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
        """
        assert booking_id or (
            client_name and client_contact
        ), "Must provide booking_id or (client_name and client_contact)"

        assert (
            reservation_datetime or table_id
        ), "Must provide reservation_datetime or table_id"

        df = self.table.query()

        # Get client booking
        client_booking = df[
            (df.booking_id == booking_id)
            | ((df.client_name == client_name) & (df.client_contact == client_contact))
        ]

        # if values not provided, use values from client_booking
        booking_id = booking_id or int(client_booking.booking_id.values[0])
        client_name = client_name or client_booking.client_name.values[0]
        client_contact = client_contact or client_booking.client_contact.values[0]
        table_id = table_id or int(client_booking.table_id.values[0])
        reservation_datetime = (
            reservation_datetime or client_booking.reservation_datetime.values[0]
        )

        # Checks new availability
        if not is_table_available(df, table_id, reservation_datetime):
            restaurantLogger.warn(
                f"Table {table_id} is not available at {reservation_datetime}"
            )
            return self

        # Update reservation details
        self.table.update_multiple(
            f"booking_id={booking_id} AND client_name='{client_name}' AND client_contact='{client_contact}'",
            col=["reservation_datetime", "table_id"],
            value=[reservation_datetime, table_id],
        )

        restaurantLogger.info(f"Updated booking {booking_id}.")

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
        """
        assert booking_id or (
            client_name and client_contact
        ), "Must provide booking_id or (client_name and client_contact)"

        if booking_id:
            self.table.delete(f"booking_id={booking_id}")
            restaurantLogger.info(f"Deleted booking {booking_id}.")
            return self

        self.table.delete(
            f"client_name='{client_name}' and client_contact='{client_contact}'"
        )
        restaurantLogger.info(f"Deleted booking for {client_name}")
        return self

    def show(self):
        self.table.show()
        return self


# ENDFILE
