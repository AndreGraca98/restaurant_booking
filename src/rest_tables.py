import logging
import sqlite3
from typing import Any, List, Tuple, Union

from .db import Table, is_int, is_str, is_valid_type
from .log import add_console_handler

restaurantTablesLogger = logging.getLogger(__name__)
add_console_handler(restaurantTablesLogger)


class RestaurantTables(Table):
    """Tables class for managing restaurant tables"""

    def __init__(
        self,
        conn: sqlite3.Connection,
        order_by: Union[str, List[str]] = None,
    ):
        super().__init__("tables", conn, order_by)

        df = self.as_df
        self.available_numbers = df.table_number.tolist()
        self.available_ids = df.table_id.tolist()

    def _normalize(self, table_number: int) -> int:
        return int(table_number)

    def add(self, table_number: int, capacity: int = 4):
        """Add a new restaurant table

        Args:
            table_number (int): Table number
            capacity (int, optional): Table capacity. Defaults to 4.

        Returns:
            _type_: Self

        Example:
            >>> RestaurantTables(db.conn).add(1)
            >>> RestaurantTables(db.conn).add(table_number=1, capacity=2)
        """
        if not is_valid_type(table_number):
            restaurantTablesLogger.error(
                f"Invalid type for table_number: type(table_number)={type(table_number).__name__}"
            )
            return self

        return super().add(
            table_number=self._normalize(table_number), capacity=capacity
        )

    def update(self, table_number: int, col: str, value: Any):
        """Update a restaurant table

        Args:
            table_number (int): Table number
            capacity (int, optional): Table capacity. Defaults to 4.

        Returns:
            _type_: Self

        Example:
            >>> RestaurantTables(db.conn).update(1, "capacity", 2)
            >>> RestaurantTables(db.conn).update(table_number=1, col="capacity", value=2)

        """
        if not is_int(value):
            restaurantTablesLogger.error(
                f"value must be an int: type(value)={type(value).__name__}"
            )
            return self

        if not is_valid_type(table_number):
            restaurantTablesLogger.error(
                f"Invalid type for table_number: type(table_number)={type(table_number).__name__}"
            )
            return self

        if not is_str(col):
            restaurantTablesLogger.error(
                f"Column must be a str: type(col)={type(col).__name__}"
            )
            return self

        return super().update(
            condition=f"table_number={self._normalize(table_number)}",
            col=col,
            value=value,
        )

    def delete(self, table_number: int):
        """Remove table by id

        Args:
            table_number (int): Table number

        Returns:
            _type_: Self

        Example:
            >>> RestaurantTables(db.conn).remove(1)
        """
        if table_number is None or table_number == []:
            restaurantTablesLogger.error(
                f"table_number must be a int: type(table_number)={type(table_number)}"
            )
            return self

        return super().delete(f"table_number={self._normalize(table_number)}")

    def get_id(self, table_number: int) -> Union[int, None]:
        """Get table by number

        Args:
            table_number (int): Table number

        Returns:
            _type_: Self

        Example:
            >>> Tables(db.conn).get_id(1)
        """
        if not is_valid_type(table_number):
            restaurantTablesLogger.error(
                f"Invalid type for table_number: type(table_number)={type(table_number).__name__}"
            )
            return None

        t_num = self._normalize(table_number)

        df = self.as_df
        query = df[df.table_number == t_num]

        # First row, table_id column
        if not query.empty:
            return int(query.table_id.values[0])

        restaurantTablesLogger.warn(f"Table not found: {t_num}")
        return None

    def exists(self, table_number: int) -> bool:
        """Check if table exists

        Args:
            table_number (int): Table number

        Returns:
            bool: True if table exists, False otherwise

        Example:
            >>> RestaurantTables(db.conn).exists(1)
        """
        return True if self.get_id(table_number) is not None else False


def create_restaurant_tables(conn: sqlite3.Connection):
    """Creates the tables for the restaurant

    Args:
        conn (sqlite3.Connection): Connection to the database
    """
    restaurantTablesLogger.debug("Creating restaurant tables")

    r_tables = RestaurantTables(conn)
    table_number = [11, 2, 3, 4]
    capacity = [4, 4, 2, 2]
    for num, cap in zip(table_number, capacity):
        r_tables.add(num, cap)

    r_tables.show()


# ENDFILE
