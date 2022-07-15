# -----------------------------------------------------------------------
# Adapted from MongoDB-labs py-tpcc package:
# https://github.com/mongodb-labs/py-tpcc/blob/1fb6f851f5668eb9f253deb209069831c9303496/pytpcc/constants.py
# -----------------------------------------------------------------------

import workloads.tpcc.functions.graph as graph

MONEY_DECIMALS: int = 2

#  Item constants
NUM_ITEMS: int = 100000
MIN_IM: int = 1
MAX_IM: int = 10000
MIN_PRICE: float = 1.00
MAX_PRICE: float = 100.00
MIN_I_NAME: int = 14
MAX_I_NAME: int = 24
MIN_I_DATA: int = 26
MAX_I_DATA: int = 50

#  Warehouse constants
MIN_TAX: int = 0
MAX_TAX: float = 0.2000
TAX_DECIMALS: int = 4
INITIAL_W_YTD: float = 300000.00
MIN_NAME: int = 6
MAX_NAME: int = 10
MIN_STREET: int = 10
MAX_STREET: int = 20
MIN_CITY: int = 10
MAX_CITY: int = 20
STATE: int = 2
ZIP_LENGTH: int = 9
ZIP_SUFFIX: str = "11111"

#  Stock constants
MIN_QUANTITY: int = 10
MAX_QUANTITY: int = 100
DIST: int = 24
STOCK_PER_WAREHOUSE: int = 100000

#  District constants
DISTRICTS_PER_WAREHOUSE: int = 10
INITIAL_D_YTD: float = 30000.00
INITIAL_NEXT_O_ID: int = 3001

#  Customer constants
CUSTOMERS_PER_DISTRICT: int = 3000
INITIAL_CREDIT_LIM: float = 50000.00
MIN_DISCOUNT: float = 0.0000
MAX_DISCOUNT: float = 0.5000
DISCOUNT_DECIMALS: int = 4
INITIAL_BALANCE: float = -10.00
INITIAL_YTD_PAYMENT: float = 10.00
INITIAL_PAYMENT_CNT: int = 1
INITIAL_DELIVERY_CNT: int = 0
MIN_FIRST: int = 6
MAX_FIRST: int = 10
MIDDLE: str = "OE"
PHONE: int = 16
MIN_C_DATA: int = 300
MAX_C_DATA: int = 500
GOOD_CREDIT: str = "GC"
BAD_CREDIT: str = "BC"

#  Order constants
MIN_CARRIER_ID: int = 1
MAX_CARRIER_ID: int = 10
#  HACK: This is not strictly correct, but it works
NULL_CARRIER_ID = 0o0
#  o_id < than this value, carrier != null, >= -> carrier == null
NULL_CARRIER_LOWER_BOUND: int = 2101
MIN_OL_CNT: int = 5
MAX_OL_CNT: int = 15
INITIAL_ALL_LOCAL: int = 1
INITIAL_ORDERS_PER_DISTRICT: int = 3000

#  Used to generate new order transactions
MAX_OL_QUANTITY: int = 10

#  Order line constants
INITIAL_QUANTITY: int = 5
MIN_AMOUNT: float = 0.01

#  History constants
MIN_DATA: int = 12
MAX_DATA: int = 24
INITIAL_AMOUNT: float = 10.00

#  New order constants
INITIAL_NEW_ORDERS_PER_DISTRICT: int = 900

#  TPC-C 2.4.3.4 (page 31) says this must be displayed when new order rolls back.
INVALID_ITEM_MESSAGE: str = "Item number is not valid"

#  Used to generate payment transactions
MIN_PAYMENT: float = 1.0
MAX_PAYMENT: float = 5000.0

#  Indicates "brand" items and stock in i_data and s_data.
ORIGINAL_STRING: str = "ORIGINAL"

# Operator Names
TABLENAME_ITEM = "ITEM"
TABLENAME_WAREHOUSE = "WAREHOUSE"
TABLENAME_DISTRICT = "DISTRICT"
TABLENAME_CUSTOMER = "CUSTOMER"
TABLENAME_STOCK = "STOCK"
TABLENAME_ORDERS = "ORDERS"
TABLENAME_NEW_ORDER = "NEW_ORDER"
TABLENAME_ORDER_LINE = "ORDER_LINE"
TABLENAME_HISTORY = "HISTORY"

FUNCTIONS_CUSTOMER = graph.customer
FUNCTIONS_DISTRICT = graph.district
FUNCTIONS_HISTORY = graph.history
FUNCTIONS_ITEM = graph.item
FUNCTIONS_NEW_ORDER = graph.new_order
FUNCTIONS_ORDER = graph.order
FUNCTIONS_ORDER_LINE = graph.order_line
FUNCTIONS_STOCK = graph.stock
FUNCTIONS_WAREHOUSE = graph.warehouse

OPERATOR_CUSTOMER = graph.customer_operator
OPERATOR_DISTRICT = graph.district_operator
OPERATOR_HISTORY = graph.history_operator
OPERATOR_ITEM = graph.item_operator
OPERATOR_NEW_ORDER = graph.new_order_operator
OPERATOR_ORDER = graph.order_operator
OPERATOR_ORDER_LINE = graph.order_line_operator
OPERATOR_STOCK = graph.stock_operator
OPERATOR_WAREHOUSE = graph.warehouse_operator

ALL_TABLES = [
    TABLENAME_ITEM,
    TABLENAME_WAREHOUSE,
    TABLENAME_DISTRICT,
    TABLENAME_CUSTOMER,
    TABLENAME_STOCK,
    TABLENAME_ORDERS,
    TABLENAME_NEW_ORDER,
    TABLENAME_ORDER_LINE,
    TABLENAME_HISTORY,
]


# Transaction Types
def enum(*sequential, **named):
    enums = dict(map(lambda x: (x, x), sequential))
    return type('Enum', (), enums)


TransactionTypes = enum(
    "NEW_ORDER",
    "PAYMENT",
)
