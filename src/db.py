import logging
import sqlite3
from typing import Any, List, Tuple, Union

import pandas as pd

from src.log import add_console_handler

dbLogger = logging.getLogger(__name__)
add_console_handler(dbLogger)


def is_str(value: Any) -> bool:
    """Check if value is of type str"""
    if isinstance(value, str):
        return True
    return False


def is_int(value: Any) -> bool:
    """Check if value is of type int"""
    if isinstance(value, int):
        return True
    return False


def is_float(value: Any) -> bool:
    """Check if value is of type float"""
    if isinstance(value, float):
        return True
    return False


def is_valid_type(value: Any) -> bool:
    """Check if value is a valid type for the database

    Args:
        value (Any): Value to be checked

    Returns:
        bool: True if value is of type str, int or float, False otherwise
    """
    if isinstance(value, (str, int, float, type(None))):
        return True
    return False


def all_valid_types(values: List[Any]) -> bool:
    """Check if all values are of valid type

    Args:
        values (List[Any]): List of values to be checked

    Returns:
        bool: True if all values are of type str, int or float, False otherwise
    """
    if all(is_valid_type(value) for value in values):
        return True
    return False


def _execute_stmt(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    stmt: str,
    values: Union[List, str, int, float] = None,
) -> None:
    """Execute an SQL statement

    Args:
        stmt (str): SQL statement
        values (List[Any], optional): Values to be inserted. Defaults to None.
    """
    if not is_str(stmt):
        dbLogger.error(f"Invalid SQL statement: {stmt}")
        return

    if values is not None and not all_valid_types(values):
        dbLogger.error(
            f"Invalid values with types: {values} » {list(map(type, values))}"
        )
        return

    # If no values, convert to empty list
    if values is None:
        values = []

    # If only one item, convert to list with one item
    if not isinstance(values, (list, tuple)):
        values = [values]

    try:
        cursor.execute(stmt, values)
        conn.commit()
        dbLogger.debug(f"{stmt} {values}")

    except (
        sqlite3.IntegrityError,  # Invalid constraint
        sqlite3.OperationalError,  # Table/column does not exist
        sqlite3.ProgrammingError,  # No parameters suplied
    ) as e:
        dbLogger.error(f"{stmt} {values} :: {e}")


class Database:
    """Database for Restaurant Management System"""

    def __init__(self, db_name: str = "restaurant"):
        self.db_name = db_name

        self.conn = sqlite3.connect(f"{self.db_name}.sqlite3")
        self.cursor = self.conn.cursor()

        self.create()

    def close(self):
        dbLogger.debug("Closing database ...")
        self.conn.close()

    def _execute(self, stmt: str, values: List[Any] = None) -> None:
        """Execute an SQL statement

        Args:
            stmt (str): SQL statement
            values (List[Any], optional): Values to be inserted. Defaults to None.
        """
        _execute_stmt(self.conn, self.cursor, stmt, values)

    def create(self):
        dbLogger.debug("Creating database ...")

        # Create client table
        self._execute(
            f"CREATE TABLE IF NOT EXISTS clients (client_id INTEGER PRIMARY KEY, client_name TEXT UNIQUE NOT NULL, client_contact TEXT UNIQUE NOT NULL);"
        )

        # Create bookings table
        self._execute(
            f"CREATE TABLE IF NOT EXISTS bookings (booking_id INTEGER PRIMARY KEY, client_id INTEGER NOT NULL, reservation_dt TEXT NOT NULL, table_id INTEGER NOT NULL);"
        )

        # Create restaurant tables table
        self._execute(
            f"CREATE TABLE IF NOT EXISTS tables (table_id INTEGER PRIMARY KEY, table_number INTEGER UNIQUE NOT NULL , capacity INTEGER NOT NULL, order_id INTEGER UNIQUE);"
        )

        # Create menu table
        self._execute(
            f"CREATE TABLE IF NOT EXISTS menu (menu_id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, price INTEGER NOT NULL);"
        )

        # Create orders table
        self._execute(
            f"CREATE TABLE IF NOT EXISTS orders (order_id INTEGER PRIMARY KEY, order_dt TEXT NOT NULL UNIQUE, paid INTEGER NOT NULL, total_price INTEGER NOT NULL);"
        )

        # Create menu_orders table to link menu and orders
        self._execute(
            f"CREATE TABLE IF NOT EXISTS menu_orders (menu_orders_id INTEGER PRIMARY KEY, menu_id INTEGER NOT NULL, order_id INTEGER NOT NULL);"
        )

        # Create kitchen table
        self._execute(
            f"CREATE TABLE IF NOT EXISTS kitchen (kitchen_id INTEGER PRIMARY KEY , order_id INTEGER, status TEXT NOT NULL);"
        )

        return self

    def show(self):
        indentation = "\n    "
        result = self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()

        table_names = sorted(map(lambda x: x[0], result))

        print(f"Schema:\n")
        for table_name in table_names:
            result = self.cursor.execute(
                f"PRAGMA table_info('{table_name}')"
            ).fetchall()
            column_names = list(zip(*result))[1]
            print((f"  {table_name}:\n    {indentation.join(column_names)}\n"))

        return self

    def delete(self):
        dbLogger.debug("Deleting database ...")
        result = self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()

        table_names = sorted(map(lambda x: x[0], result))
        for table_name in table_names:
            self._execute(f"DROP TABLE IF EXISTS '{table_name}';")

        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.db_name})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.db_name})"


class Table:
    """Table class for database operations"""

    def __init__(self, table_name: str, conn: sqlite3.Connection):
        self.table_name = table_name
        self.conn = conn
        self.cursor = self.conn.cursor()

    @property
    def as_df(self) -> pd.DataFrame:
        """Returns a pandas DataFrame of the table

        Returns:
            pd.DataFrame: Query result

        Example:
            >>> table.as_df
        """

        stmt = f"SELECT * FROM '{self.table_name}';"
        return pd.read_sql_query(stmt, self.conn)

    def _execute(self, stmt: str, values: List[Any] = None) -> None:
        """Execute an SQL statement

        Args:
            stmt (str): SQL statement
            values (List[Any], optional): Values to be inserted. Defaults to None.
        """
        _execute_stmt(self.conn, self.cursor, stmt, values)

    def select(self, stmt: str) -> List[Any]:
        """Selects rows from table

        Args:
            stmt (str): SQL statement

        Returns:
            List[Any]: Query result

        Example:
            >>> table.select("SELECT * FROM table")
        """
        if not is_str(stmt):
            dbLogger.error(f"Invalid SQL statement: {stmt}")
            return []

        stmt = stmt.strip()  # Remove whitespaces
        if stmt[~0] != ";":
            stmt += ";"  # Add semicolon if not present

        if stmt[:6].upper() != "SELECT":
            dbLogger.error(f"Only SELECT statements are allowed: {stmt}")
            return []

        self._execute(stmt=stmt)
        result = self.cursor.fetchall()
        return result

    def add(self, **items):
        """Adds a row to table

        kwargs are used to specify column names and values

        Example:
            >>> table.add(price=1, name="a")
            >>> # INSERT INTO table (price, name) VALUES (1 , "a");
        """
        if not items:
            dbLogger.debug(f"No entry to add")
            return self

        cols = ", ".join(list(map(lambda x: f"'{x}'", items.keys())))
        values = list(items.values())
        qm = ", ".join(["?"] * len(values))

        stmt = f"INSERT INTO '{self.table_name}' ({cols}) VALUES ({qm});"

        self._execute(stmt, values)

        return self

    def add_multiple(self, **items: List[Any]):
        """Adds multiple rows to table

        kwargs are used to specify column names and values. Each column has a list of values.

        Example:
            >>> table.add_multiple(price=[1, 2, 3], name=["a", "b", "c"])
            >>> # INSERT INTO table (price, name) VALUES (1 , "a");
            >>> # INSERT INTO table (price, name) VALUES (2 , "b");
            >>> # INSERT INTO table (price, name) VALUES (3 , "c");
        """
        if not items:
            dbLogger.debug(f"No entry to add")
            return self

        if not all(len(v) == len(list(items.values())[0]) for v in items.values()):
            dbLogger.error(f"Length of lists are not equal : {items}")
            return self

        cols = ", ".join(list(map(lambda x: f"'{x}'", items.keys())))
        values_list = list(items.values())
        qm = ", ".join(["?"] * len(values_list))

        for values in zip(*values_list):
            stmt = f"INSERT INTO '{self.table_name}' ({cols}) VALUES ({qm});"
            self._execute(stmt, values)

        return self

    def delete(self, condition: str = None):
        """Deletes from table based on condition. If no condition is specified, all rows are deleted.

        Args:
            condition (str): Condition for deletion

        Example:
            >>> table.delete("booking_id=booking_id AND name=name")
            >>> # DELETE FROM table WHERE booking_id=booking_id AND name=name;
            >>> table.delete()
            >>> # DELETE FROM table;
        """
        if condition is not None and not is_str(condition):
            dbLogger.error(f"Invalid condition: {condition}")
            return self

        stmt = f"DELETE FROM '{self.table_name}' "
        stmt += ";" if condition is None else f"WHERE {condition};"

        self._execute(stmt)

        return self

    def update(self, condition: str, col: str, value: Any):
        """Updates a column in a table

        Args:
            condition (str): Condition for the update
            col (str): Column to update
            value (Any): New value

        Example:
            >>> table.update("booking_id=booking_id AND name=name", col="reservation_datetime", value="2023-01-01 12:00:00")
            >>> # UPDATE table SET 'reservation_datetime' = 2023-01-01 12:00:00 WHERE booking_id=booking_id AND name=name;
        """
        if not is_str(condition) or condition == "":
            dbLogger.error(f"Invalid condition: {condition}")
            return self

        stmt = f"UPDATE '{self.table_name}' SET '{col}' = ? WHERE {condition};"

        self._execute(stmt, [value])

        return self

    def update_multiple(
        self, condition: str, cols: List[str], values: List[Any]
    ) -> None:
        """Updates multiple columns in a table

        Args:
            condition (str): Condition for the update
            cols (List[str]): Columns to update
            values (List[Any]): New values

        Example:
            >>> table.update_multiple("booking_id=booking_id AND name=name", cols=["reservation_datetime", "table_id"], values=["2023-01-01 12:00:00", 1])
            >>> # UPDATE table SET 'reservation_datetime' = 2023-01-01 12:00:00 WHERE booking_id=booking_id AND name=name;
            >>> # UPDATE table SET 'table_id' = 1 WHERE booking_id=booking_id AND name=name;

        """
        if not is_str(condition) or condition == "":
            dbLogger.error(f"Invalid condition: {condition}")
            return self

        if not isinstance(cols, (list, tuple)) or not isinstance(values, (list, tuple)):
            dbLogger.error(
                f"Columns and values must be of type list: cols={type(cols)} ; values={type(values)}"
            )
            return self

        if len(cols) != len(values):
            dbLogger.error(
                f"Number of columns and values do not match: len(cols)={len(cols)} ; len(values)={len(values)}"
            )
            return self

        for col, value in zip(cols, values):
            stmt = f"UPDATE '{self.table_name}' SET '{col}' = ? WHERE {condition};"
            self._execute(stmt, [value])

        return self

    def show(self):
        """Prints the table

        Returns:
            Table: Table object

        Example:
            >>> table.show()
        """
        dbLogger.info(str(self))
        return self

    def __str__(self) -> str:
        df = self.as_df
        return f"""Table: {self.table_name}
Shape: {df.shape}
================================================================================
{df if not df.empty else '    ' + ', '.join(list(df.columns))}
================================================================================
"""

    def __repr__(self) -> str:
        df = self.as_df
        return f"Table: {self.table_name} ; Shape: {df.shape} ; Columns: {list(df.columns)}"


def create_restaurant_menu(conn: sqlite3.Connection):
    """Creates the menu for the restaurant

    Args:
        conn (sqlite3.Connection): Connection to the database
    """

    menu = Table("menu", conn)
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

    dbLogger.debug(repr(menu))


def add_item_to_menu(conn: sqlite3.Connection, name: str, price: int):
    """Adds an item to the menu

    Args:
        conn (sqlite3.Connection): Connection to the database
        name (str): Name of the item
        price (int): Price of the item
    """
    menu = Table("menu", conn)
    menu.add(name=name.title(), price=int(price))

    dbLogger.debug(repr(menu))


def create_restaurant_tables(conn: sqlite3.Connection):
    """Creates the tables for the restaurant

    Args:
        conn (sqlite3.Connection): Connection to the database
    """
    tables = Table("tables", conn)
    tables.add_multiple(
        table_number=[11, 2, 3, 4],
        capacity=[4, 4, 2, 2],
    )

    dbLogger.debug(repr(tables))


# ENDFILE
