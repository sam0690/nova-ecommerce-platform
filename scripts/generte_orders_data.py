import csv
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


DATA_DIR = Path("data")

PRODUCTS = [
    {
        "product_id": "1",
        "product_name": "Wireless Mouse",
        "category": "Electronics",
        "unit_price": Decimal("29.99"),
    },
    {
        "product_id": "2",
        "product_name": "Mechanical Keyboard",
        "category": "Electronics",
        "unit_price": Decimal("89.99"),
    },
    {
        "product_id": "3",
        "product_name": "USB-C Hub",
        "category": "Electronics",
        "unit_price": Decimal("49.99"),
    },
    {
        "product_id": "4",
        "product_name": "Coffee Maker",
        "category": "Home",
        "unit_price": Decimal("79.99"),
    },
    {
        "product_id": "5",
        "product_name": "Desk Lamp",
        "category": "Home",
        "unit_price": Decimal("34.99"),
    },
    {
        "product_id": "6",
        "product_name": "Running Shoes",
        "category": "Sports",
        "unit_price": Decimal("99.99"),
    },
    {
        "product_id": "7",
        "product_name": "Yoga Mat",
        "category": "Sports",
        "unit_price": Decimal("24.99"),
    },
    {
        "product_id": "8",
        "product_name": "Cotton T-Shirt",
        "category": "Clothing",
        "unit_price": Decimal("19.99"),
    },
    {
        "product_id": "9",
        "product_name": "Denim Jeans",
        "category": "Clothing",
        "unit_price": Decimal("59.99"),
    },
    {
        "product_id": "10",
        "product_name": "Python Programming Book",
        "category": "Books",
        "unit_price": Decimal("39.99"),
    },
]

STATUSES = [
    "completed",
    "pending",
    "cancelled",
    "refunded",
]

FIELDNAMES = [
    "order_id",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "amount",
    "status",
    "order_date",
]


def generate_valid_row(order_number: int, date: datetime) -> dict:
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)

    unit_price = product["unit_price"]
    amount = unit_price * quantity

    random_seconds = random.randint(0, 86399)

    order_date = date + timedelta(seconds=random_seconds)

    return {
        "order_id": f"ORD-{order_number:06d}",
        "customer_id": str(random.randint(1, 10)),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": str(unit_price),
        "amount": str(amount),
        "status": random.choice(STATUSES),
        "order_date": order_date.isoformat(),
    }


def make_invalid_row(row: dict, invalid_type: str) -> dict:
    row = row.copy()

    if invalid_type == "negative_amount":
        row["amount"] = "-50.00"

    elif invalid_type == "unknown_status":
        row["status"] = "unknown_status"

    elif invalid_type == "invalid_date":
        row["order_date"] = "not-a-valid-date"

    return row


def generate_file(
    filename: str,
    date_string: str,
    row_count: int,
    invalid_percentage: float = 0,
    starting_order_number: int = 1,
):
    DATA_DIR.mkdir(exist_ok=True)

    date = datetime.fromisoformat(
        date_string
    ).replace(tzinfo=timezone.utc)

    file_path = DATA_DIR / filename

    invalid_count = int(
        row_count * invalid_percentage
    )

    invalid_indexes = set(
        random.sample(
            range(row_count),
            invalid_count,
        )
    )

    invalid_types = [
        "negative_amount",
        "unknown_status",
        "invalid_date",
    ]

    with open(
        file_path,
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=FIELDNAMES,
        )

        writer.writeheader()

        for i in range(row_count):

            row = generate_valid_row(
                order_number=starting_order_number + i,
                date=date,
            )

            if i in invalid_indexes:
                invalid_type = random.choice(
                    invalid_types
                )

                row = make_invalid_row(
                    row,
                    invalid_type,
                )

            writer.writerow(row)

    print(
        f"Created {file_path} "
        f"with {row_count} rows "
        f"({invalid_count} intentionally invalid)"
    )


def main():
    random.seed(42)

    generate_file(
        filename="orders_2026-08-01.csv",
        date_string="2026-08-01",
        row_count=2500,
        invalid_percentage=0.005,
        starting_order_number=1,
    )

    generate_file(
        filename="orders_2026-08-02.csv",
        date_string="2026-08-02",
        row_count=1800,
        invalid_percentage=0,
        starting_order_number=2501,
    )

    generate_file(
        filename="orders_2026-08-03.csv",
        date_string="2026-08-03",
        row_count=2200,
        invalid_percentage=0,
        starting_order_number=4301,
    )

    generate_file(
        filename="orders_2026-08-04_bad.csv",
        date_string="2026-08-04",
        row_count=1000,
        invalid_percentage=0.20,
        starting_order_number=6501,
    )


if __name__ == "__main__":
    main()
