import csv
import random
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


DATA_DIR = Path("data")

# Mirrors sql/ddl/002_seed.sql exactly (ids follow the seed's INSERT order).
# If you change one, change the other, or category charts split a product two ways.
PRODUCTS = [
    {"product_id": "1", "product_name": "Mechanical Keyboard 75%", "category": "peripherals", "unit_price": Decimal("129.00")},
    {"product_id": "2", "product_name": "Wireless Mouse", "category": "peripherals", "unit_price": Decimal("49.50")},
    {"product_id": "3", "product_name": "27in 1440p Monitor", "category": "displays", "unit_price": Decimal("319.99")},
    {"product_id": "4", "product_name": "34in Ultrawide Monitor", "category": "displays", "unit_price": Decimal("579.00")},
    {"product_id": "5", "product_name": "Over-ear Headphones", "category": "audio", "unit_price": Decimal("199.95")},
    {"product_id": "6", "product_name": "Desk Speakers Pair", "category": "audio", "unit_price": Decimal("89.00")},
    {"product_id": "7", "product_name": "Standing Desk 120cm", "category": "furniture", "unit_price": Decimal("449.00")},
    {"product_id": "8", "product_name": "Ergonomic Chair", "category": "furniture", "unit_price": Decimal("612.75")},
    {"product_id": "9", "product_name": "USB-C Hub 8-in-1", "category": "accessories", "unit_price": Decimal("39.99")},
    {"product_id": "10", "product_name": "Laptop Stand Aluminium", "category": "accessories", "unit_price": Decimal("27.50")},
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

    start_date = date(2026, 7, 1)
    days = 56

    # Weekday rhythm: quiet midweek, busy weekend. Monday=0.
    weekday_volume = [1700, 1600, 1650, 1800, 2100, 2500, 2300]

    order_number = 1

    for offset in range(days):
        day = start_date + timedelta(days=offset)

        # Gentle upward drift: +0.5% per day compounding, plus daily noise.
        drift = 1.005 ** offset
        noise = random.uniform(0.92, 1.08)
        row_count = int(weekday_volume[day.weekday()] * drift * noise)

        generate_file(
            filename=f"orders_{day.isoformat()}.csv",
            date_string=day.isoformat(),
            row_count=row_count,
            invalid_percentage=0.003,
            starting_order_number=order_number,
        )

        order_number += row_count


if __name__ == "__main__":
    main()
