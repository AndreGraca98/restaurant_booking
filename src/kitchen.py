import logging
import sqlite3

import pandas as pd

from .db import Table
from .log import add_console_handler

kitchenLogger = logging.getLogger(__name__)
add_console_handler(kitchenLogger)


order_status = {
    "Pending": "Cooking",
    "Cooking": "Ready",
    "Ready": "Served",
    "Served": "Served",
}


class Kitchen:
    def __init__(self, conn: sqlite3.Connection):
        self.kitchen_table = Table("kitchen", conn)
        self.orders_table = Table("orders", conn)

    def update_status(self, order_id: int):
        """Update order status

        Args:
            order_id (int): Order id
            status (OrderStatus): Order status

        Returns:
            _type_: Self

        Example:
            >>> Kitchen(conn).update(1, OrderStatus.READY)
        """

        df_o = self.orders_table.as_df
        if order_id not in df_o.order_id.values.tolist():
            kitchenLogger.info(f"Invalid order {order_id} .")
            return self

        df_k = self.kitchen_table.as_df

        kitchenLogger.debug(df_k[df_k.order_id == order_id].status)

        status = str(df_k[df_k.order_id == order_id].status.values[0])

        new_status = order_status[status]

        self.kitchen_table.update(
            f"order_id={order_id}", col="status", value=new_status
        )

        kitchenLogger.info(f"Updated order {order_id} status: {new_status}.")

        return self

    def show(self):
        """Show kitchen status

        Returns:
            _type_: Self

        Example:
            >>> Kitchen(conn).show()
        """
        self.kitchen_table.show()
        return self

    def orders(self):
        """Show all orders

        Returns:
            _type_: Self

        Example:
            >>> Kitchen(conn).orders()
        """

        values = self.kitchen_table.select(
            "SELECT orders.order_id, orders.order_dt, kitchen.status FROM orders INNER JOIN kitchen ON orders.order_id = kitchen.order_id;"
        )

        df_joint = (
            pd.DataFrame(values, columns=["order_id", "order_dt", "status"])
            .sort_values("order_dt")
            .reset_index(drop=True)
        )

        spacer = "================================================================================"
        kitchenLogger.info(
            f"Showing all orders...\nShape: {df_joint.shape}\n{spacer}\n{df_joint}\n{spacer}"
        )

        return self

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({str(self.kitchen_table)})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self.kitchen_table)})"


# ENDFILE
