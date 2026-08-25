from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator

VALID_STATUSES = {
	"pending",
	"completed",
	"cancelled",
	"refunded",
}

class Order(BaseModel):
	order_id: str
	customer_id: int
	product_id: int

	product_name: str
	category: str

	quantity: int = Field(gt=0)

	unit_price: Decimal = Field(ge=0)
	amount: Decimal = Field(ge=0)

	status: str
	order_date: datetime

	@field_validator("status")
	@classmethod
	def validate_status(cls,value: str) -> str:
		if value not in VALID_STATUSES:
			raise ValueError(
				f"unknown status:{value}"
			)
		return value











