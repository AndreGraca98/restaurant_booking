import logging
import sqlite3
from typing import Any, List, Tuple

import pandas as pd

from src.log import add_console_handler

dbLogger = logging.getLogger(__name__)
add_console_handler(dbLogger)


def query(conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    """Query a Table

    Args:
        conn (sqlite3.Connection): Database connection
        table_name (str): Table name

    Returns:
        pd.DataFrame: Query result
    """

    stmt = f"SELECT * FROM '{table_name}';"
    return pd.read_sql_query(stmt, conn)

    cursor = conn.cursor()

    cursor.execute(stmt)
    # cursor.execute(f"SELECT * FROM {table_name} ORDER BY name ASC;")
    menu = cursor.fetchall()
    # menu = cursor.fetchmany()
    # menu = cursor.fetchone()
    return menu


class Database:
    """Database for Restaurant Management System"""

    def __init__(self, db_name: str = "restaurant"):
        self.db_name = db_name

        self.conn = sqlite3.connect(f"{self.db_name}.sqlite3")
        self.cursor = self.conn.cursor()

        self.create()

    def close(self):
        self.conn.close()

    def create(self):
        dbLogger.debug("Creating database ...")
        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS menu (menu_id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, price INTEGER NOT NULL);"
        )
        self.conn.commit()

        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS orders (order_id INTEGER PRIMARY KEY, order_datetime TEXT NOT NULL UNIQUE, paid INTEGER NOT NULL, total_price INTEGER);"
        )
        self.conn.commit()

        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS menu_orders (menu_orders_id INTEGER PRIMARY KEY, menu_id INTEGER NOT NULL, order_id INTEGER NOT NULL);"
        )
        self.conn.commit()

        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS tables (table_id INTEGER PRIMARY KEY, table_number INTEGER NOT NULL UNIQUE, capacity INTEGER NOT NULL, order_id INTEGER UNIQUE);"
        )
        self.conn.commit()

        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS bookings (booking_id INTEGER PRIMARY KEY, client_name TEXT UNIQUE, client_contact TEXT UNIQUE, reservation_datetime TEXT NOT NULL, table_number INTEGER NOT NULL);"
        )
        self.conn.commit()

        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS kitchen (kitchen_id INTEGER PRIMARY KEY , order_id INTEGER, status TEXT);"
        )
        self.conn.commit()

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
            self.cursor.execute(f"DROP TABLE IF EXISTS '{table_name}';")
            self.conn.commit()

        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


class Table:
    """Table class for database operations"""

    def __init__(self, table_name: str, conn: sqlite3.Connection):
        self.table_name = table_name
        self.conn = conn
        self.cursor = self.conn.cursor()

    def select(self, stmt: str):
        self.cursor.execute(stmt)
        dbLogger.debug(f"{stmt}")
        return self.cursor.fetchall()

    def get_df(self) -> pd.DataFrame:
        """Query a Table

        Returns:
            pd.DataFrame: Query result

        Example:
            >>> table.get_df()
        """

        stmt = f"SELECT * FROM '{self.table_name}';"
        return pd.read_sql_query(stmt, self.conn)

    def add(self, **items):
        """Adds a row to table

        kwargs are used to specify column names and values

        Example:
            >>> table.add(price=1, name="a")
        """
        cols = ", ".join(list(map(lambda x: f"'{x}'", items.keys())))
        values = list(items.values())
        qm = ", ".join(["?"] * len(values))

        stmt = f"INSERT INTO '{self.table_name}' ({cols}) VALUES ({qm});"

        try:
            self.cursor.execute(stmt, values)
            dbLogger.debug(f"{stmt} {values}")
            self.conn.commit()
        except sqlite3.IntegrityError:
            dbLogger.debug(f"Duplicate entry: {values}")

        return self

    def add_multiple(self, **items: List[Any]):
        """Adds multiple rows to table

        kwargs are used to specify column names and values. Each column has a list of values.

        Example:
            >>> table.add_multiple(price=[1, 2, 3], name=["a", "b", "c"])
        """
        cols = ", ".join(list(map(lambda x: f"'{x}'", items.keys())))
        values_list = list(items.values())
        qm = ", ".join(["?"] * len(values_list))

        for values in zip(*values_list):
            stmt = f"INSERT INTO '{self.table_name}' ({cols}) VALUES ({qm});"
            try:
                self.cursor.execute(stmt, values)
                dbLogger.debug(f"{stmt} {values}")
            except sqlite3.IntegrityError:
                dbLogger.debug(f"Duplicate entry: {values}")

        self.conn.commit()

        return self

    def delete(self, condition: str = None):
        """Deletes from table based on condition. If no condition is specified, all rows are deleted.

        Args:
            condition (str): Condition for deletion

        Example:
            >>> table.delete("booking_id=booking_id AND name=name")
            >>> table.delete()
        """
        if condition:
            stmt = f"DELETE FROM '{self.table_name}' WHERE {condition};"
        else:
            stmt = f"DELETE FROM '{self.table_name}';"

        dbLogger.debug(stmt)
        self.cursor.execute(stmt)

        self.conn.commit()

        return self

    def update(self, condition: str, col: str, value: Any):
        """Updates a column in a table

        Args:
            condition (str): Condition for the update
            col (str): Column to update
            value (Any): New value

        Example:
            >>> table.update("booking_id=booking_id AND name=name", col="reservation_datetime", value="2023-01-01 12:00:00")
        """

        stmt = f"UPDATE '{self.table_name}' SET '{col}' = ? WHERE {condition};"
        dbLogger.debug(f"{stmt} {[value]}")
        self.cursor.execute(stmt, [value])

        self.conn.commit()

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
        """

        for col, value in zip(cols, values):
            stmt = f"UPDATE '{self.table_name}' SET '{col}' = ? WHERE {condition};"
            try:
                self.cursor.execute(stmt, [value])
                dbLogger.debug(f"{stmt} {[value]}")
            except sqlite3.IntegrityError:
                dbLogger.debug(f"Duplicate entry: {value}")

        self.conn.commit()

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
        query = self.get_df()
        return f"""Table: {self.table_name}
Shape: {query.shape}
================================================================================
{query if not query.empty else '    ' + ', '.join(list(query.columns))}
================================================================================
"""

    def __repr__(self) -> str:
        query = self.get_df()
        return f"Table: {self.table_name} ; Shape: {query.shape} ; Columns: {list(query.columns)}"


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
