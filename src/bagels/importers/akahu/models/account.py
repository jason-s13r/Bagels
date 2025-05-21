from datetime import datetime
from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
import akahu.models.account as models
from bagels.models.database.app import Base


class AkahuAccount(Base):
    __tablename__ = "akahu_account"
    akahuId = Column(String, primary_key=True)
    accountId = Column(Integer, ForeignKey("account.id"), nullable=True)
    createdAt = Column(DateTime, nullable=False, default=datetime.now())
    updatedAt = Column(DateTime, nullable=False, default=datetime.now())

    name = Column(String, nullable=False)
    description = Column(String)
    beginningBalance = Column(Float, nullable=False)
    repaymentDate = Column(Integer)
    hidden = Column(Boolean, nullable=False, default=False)

    account = relationship("Account", foreign_keys=[accountId])

    @staticmethod
    def create_from_akahu(account: models.Account) -> "AkahuAccount":
        provider = account.connection.name
        holder = "" if account.meta is None else account.meta.holder or ""
        name = account.name.replace(holder, "").strip()
        formatted_name = account.formatted_account or account.name
        description = f"{provider} - {formatted_name}"

        balance = account.balance.current if account.balance else 0
        balance_float = float(balance)
        hidden = account.status == "INACTIVE"

        return AkahuAccount(
            akahuId=account.id,
            name=name,
            description=description,
            beginningBalance=balance_float,
            hidden=hidden,
        )

    def update_from_akahu(self, account: models.Account) -> "AkahuAccount":
        changes = AkahuAccount.create_from_akahu(account)

        self.name = changes.name
        self.description = changes.description
        self.beginningBalance = changes.beginningBalance
        self.hidden = changes.hidden
        self.updatedAt = datetime.now()

        return self
