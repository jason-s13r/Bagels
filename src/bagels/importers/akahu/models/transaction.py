from datetime import datetime
import math
from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    Boolean,
    DateTime,
    ForeignKey,
    CheckConstraint,
)
from sqlalchemy.orm import relationship
import akahu.models.transaction as models
from bagels.models.database.app import Base


class AkahuTransaction(Base):
    __tablename__ = "akahu_transaction"
    akahuId = Column(String, primary_key=True)
    akahuAccountId = Column(
        Integer, ForeignKey("akahu_account.akahuId"), nullable=False
    )
    akahuCategoryId = Column(
        Integer, ForeignKey("akahu_category.akahuId"), nullable=True
    )
    recordId = Column(Integer, ForeignKey("record.id"), nullable=True)
    createdAt = Column(DateTime, nullable=False, default=datetime.now())
    updatedAt = Column(DateTime, nullable=False, default=datetime.now())
    label = Column(String, nullable=False)
    amount = Column(Float, CheckConstraint("amount > 0"), nullable=False)
    date = Column(DateTime, nullable=False, default=datetime.now)
    isIncome = Column(Boolean, nullable=False, default=False)
    isInProgress = Column(Boolean, nullable=False, default=False)
    tags = Column(String, nullable=True)

    account = relationship("AkahuAccount", foreign_keys=[akahuAccountId])
    category = relationship("AkahuCategory", foreign_keys=[akahuCategoryId])
    record = relationship("Record", foreign_keys=[recordId])

    @staticmethod
    def create_from_akahu(transaction: models.Transaction) -> "AkahuTransaction":
        categoryId = transaction.category.id if transaction.category else None
        accountId = transaction.account
        merchant = transaction.merchant.name if transaction.merchant else None
        label = (
            f"{merchant}: {transaction.description}"
            if merchant
            else transaction.description
        )
        tags = {"type": transaction.type, "imported": "akahu"}
        if transaction.merchant:
            tags.update({"merchant": transaction.merchant.name})
        if transaction.meta:
            card = transaction.meta.card_suffix
            reference = " ".join(
                [
                    transaction.meta.particulars or "",
                    transaction.meta.code or "",
                    transaction.meta.reference or "",
                ]
            ).strip()
            if card:
                tags.update({"card": card})
            if reference:
                tags.update({"reference": reference})
            if transaction.meta.conversion:
                fxA = transaction.meta.conversion.amount
                fxR = transaction.meta.conversion.rate
                rxC = transaction.meta.conversion.currency
                if fxA is not None and fxR is not None and rxC is not None:
                    tags.update({"conversion": f"{rxC} {fxA} @ {fxR}"})
        tagsStr = ", ".join([f"{k}: {v}" for k, v in tags.items()])
        return AkahuTransaction(
            akahuId=transaction.id,
            akahuCategoryId=categoryId,
            akahuAccountId=accountId,
            label=label,
            date=transaction.date,
            amount=math.fabs(transaction.amount),
            isIncome=transaction.amount > 0,
            tags=tagsStr,
        )

    def update_from_akahu(self, category: models.Transaction) -> "AkahuTransaction":
        changes = AkahuTransaction.create_from_akahu(category)
        self.akahuCategoryId = changes.akahuCategoryId
        self.akahuAccountId = changes.akahuAccountId
        self.label = changes.label
        self.date = changes.date
        self.amount = changes.amount
        self.isIncome = changes.isIncome
        self.tags = changes.tags
        return self
