import datetime
import logging
import sqlite3
import time
from enum import Enum
from typing import List, Union

import pandas as pd

from .db import Table
from .log import add_console_handler

orderLogger = logging.getLogger(__name__)
add_console_handler(orderLogger)


class OrderStatus(Enum):
    PENDING = "Pending"
    COOKING = "Cooking"
    READY = "Ready"
    SERVED = "Served"


def item_in_order(menu_orders_df: pd.DataFrame, order_id: int, menu_id: int) -> bool:
    """Check if item is in order

    Args:
        menu_orders_df (pd.DataFrame): Menu orders dataframe
        order_id (int): Order id
        menu_id (int): Menu item id

    Returns:
        bool: True if item is in order, False otherwise

    Example:
        >>> item_in_order(menu_orders_df, 1, 1)
    """
    if (
        menu_id
        in menu_orders_df[menu_orders_df.order_id == order_id].menu_id.values.tolist()
    ):
        return True
    return False


class Orders:
    """Orders class for managing orders from menu items"""

    def __init__(self, conn: sqlite3.Connection):
        self.orders_table = Table("orders", conn)
        self.menu_orders_table = Table("menu_orders", conn)
        self.menu_table = Table("menu", conn)
        self.kitchen_table = Table("kitchen", conn)
        self.tables_table = Table("tables", conn)

        self.menu_df = self.menu_table.get_df()

    def add(self, table_number: int, items: List[Union[int, str]]):
        """Add order to database

        Args:
            table_number (int): Table number
            items (List[Union[int, str]]): List of menu items

        Returns:
            _type_: Self

        Example:
            >>> Orders(conn).add(table_number=1, items=[1, 2, 3])
            >>> Orders(conn).add(table_number=1, items=["Burger", "Fries", "Coke"])
            >>> Orders(conn).add(table_number=1, items=[1, "Fries", 3])
        """
        order_datetime = datetime.datetime.now().isoformat(
            sep=" ", timespec="milliseconds"
        )

        # Add order to orders table
        self.orders_table.add(order_datetime=order_datetime)
        df_o = self.orders_table.get_df()
        current_order_id = df_o[
            df_o.order_datetime == order_datetime
        ].order_id.values.tolist()[0]

        # Add status to kitchen table
        self.kitchen_table.add(
            order_id=current_order_id,
            status=OrderStatus.PENDING.value,
        )

        # Update tables order_id
        self.tables_table.update(
            f"table_number={table_number}",
            col="order_id",
            value=current_order_id,
        )

        orderLogger.debug(f"Current order id: {current_order_id}")
        # Add menu items to menu_orders table
        for item in items:
            # Adding by item index
            if item in self.menu_df.menu_id.values.tolist():
                menu_id = item
            # Adding by item name
            elif str(item).title() in self.menu_df.name.values.tolist():
                menu_id = self.menu_df[
                    self.menu_df.name == item.title()
                ].menu_id.values.tolist()[0]
            else:
                orderLogger.warn(f"Item {item} does not exist.")
                continue

            if item_in_order(
                self.menu_orders_table.get_df(), current_order_id, menu_id
            ):
                orderLogger.warn(f"Item {item} already in order {current_order_id}")
                continue

            self.menu_orders_table.add(
                order_id=current_order_id,
                menu_id=menu_id,
            )
            orderLogger.info(f"Added {str(item).title()} to order {current_order_id}")

        time.sleep(0.1)  # To avoid saving different orders with same id

        # If order is empty remove it
        df_mo = self.menu_orders_table.get_df()
        if df_mo[df_mo.order_id == current_order_id].empty:
            orderLogger.warn(f"Order {current_order_id} is empty. Deleting order.")
            self.delete(order_id=current_order_id)

    def delete(
        self,
        order_id: int = None,
        order_datetime: str = None,
    ):
        """Delete order

        Args:
            order_id (int, optional): Order id. Defaults to None.
            order_datetime (str, optional): Order datetime. Defaults to None.

        Returns:
            _type_: Self

        Example:
            >>> Order(conn).delete(order_id=1)
            >>> Order(conn).delete(order_datetime="2023-02-01 12:00:00)
        """
        if not order_id and not order_datetime:
            orderLogger.warn(
                "Must provide either order_id or order_datetime to delete an order."
            )
            return self

        if not order_id:
            df_o = self.orders_table.get_df()
            df_o = df_o[df_o.order_datetime == order_datetime].order_id.values.tolist()
            if not df_o:
                orderLogger.warn(
                    f"Order with order_datetime {order_datetime} does not exist."
                )
                return self

        if order_id not in self.orders_table.get_df().order_id.values.tolist():
            orderLogger.warn(f"Order {order_id} does not exist.")
            return self

        df_k = self.kitchen_table.get_df()

        orderLogger.debug(
            f"Order status: {df_k[df_k.order_id == order_id].status.values.tolist()[0]}"
        )

        if (
            df_k[df_k.order_id == order_id].status.values.tolist()[0]
            != OrderStatus.PENDING.value
        ):
            orderLogger.warn(
                f"Order {order_id} is not pending anymore. Cannot cancel order!"
            )
            return self

        self.orders_table.delete(f"order_id={order_id}")
        self.menu_orders_table.delete(f"order_id={order_id}")
        self.kitchen_table.delete(f"order_id={order_id}")
        self.tables_table.delete(f"order_id={order_id}")
        orderLogger.info(f"Deleted order {order_id}")
        return self

    def show(self):
        """Show all orders

        Returns:
            _type_: Self

        Example:
            >>> Orders(conn).show()
        """
        self.orders_table.show()
        return self

    def menu(self):
        """Show menu

        Returns:
            _type_: Self

        Example:
            >>> Orders(conn).menu()
        """
        self.menu_table.show()
        return self

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({str(self.orders_table)})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self.orders_table)})"


def create_dummy_orders(conn: sqlite3.Connection):
    orders = Orders(conn)

    orders.add(1, [1, 3, 4, "Fries"])
    orders.add(1, [1, 2, "chicken", "salad"])
    orders.add(2, [1, 2, "chicken", "salad"])
    # orders.add(3, [999, "item that doesnt exist"])

    orderLogger.debug(repr(orders))


# ENDFILE
