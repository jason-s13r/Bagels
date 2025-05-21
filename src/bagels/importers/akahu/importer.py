import math
import itertools
from datetime import datetime
from pathlib import Path
from typing import List
from akahu.models.transaction import (
    Transaction as AkahuApiTransaction,
    PendingTransaction as AkahuApiPendingTransaction,
)
from akahu.models.account import Account as AkahuApiAccount
from akahu.client import Client as AkahuClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bagels.locations import database_file
from bagels.models.account import Account
from bagels.models.category import Category, Nature
from bagels.models.record import Record
from .models.account import AkahuAccount
from .models.group import AkahuGroup
from .models.category import AkahuCategory
from .models.transaction import AkahuTransaction
import click


class BagelsImporter:
    def run(self):
        raise NotImplementedError("Subclasses should implement this method.")


class AkahuImporter(BagelsImporter):
    def __init__(
        self, app_token: str, user_token: str, db_file: Path | None = database_file()
    ):
        self._engine = create_engine(f"sqlite:///{db_file.resolve()}")
        self._session = sessionmaker(bind=self._engine)
        self._akahu = AkahuClient(AkahuClient.Config(app_token, user_token))

    def import_accounts(self, session, accounts: List[AkahuApiAccount]):
        importIds = [account.id for account in accounts]
        query_existing = session.query(AkahuAccount).filter(
            AkahuAccount.akahuId.in_(importIds)
        )
        existing = {e.akahuId: e for e in query_existing.all()}
        changes = [
            (
                existing[account.id].update_from_akahu(account)
                if account.id in existing
                else AkahuAccount.create_from_akahu(account)
            )
            for account in accounts
        ]
        session.add_all(changes)
        session.commit()

    def sync_accounts(self, session):
        accounts = session.query(AkahuAccount).all()
        for change in accounts:
            if change.account is None:
                change.account = Account(
                    name=change.name,
                    description=change.description,
                    beginningBalance=0,
                    hidden=change.hidden,
                )
                session.add(change.account)
                session.flush()
            elif change.updatedAt < change.account.updatedAt:
                continue
            else:
                change.account.name = change.name
                change.account.description = change.description
                change.account.hidden = change.hidden
            change.account.updatedAt = datetime.now()
            change.updatedAt = change.account.updatedAt
        session.commit()

    def import_groups(self, session, transactions: List[AkahuApiTransaction]):
        groups = {
            tx.category.groups.personal_finance.id: tx.category.groups.personal_finance
            for tx in transactions
            if tx.category is not None
            and tx.category.groups is not None
            and tx.category.groups.personal_finance is not None
        }
        importIds = groups.keys()
        query_existing = session.query(AkahuGroup).filter(
            AkahuGroup.akahuId.in_(importIds)
        )
        existing = {e.akahuId: e for e in query_existing.all()}
        changes = [
            (
                existing[group.id].update_from_akahu(group)
                if group.id in existing
                else AkahuGroup.create_from_akahu(group)
            )
            for group in groups.values()
        ]
        session.add_all(changes)
        session.commit()

    def sync_groups(self, session):
        groups = session.query(AkahuGroup).all()
        parents = session.query(Category).filter_by(parentCategoryId=None).all()
        parentsDict = {parent.name: parent for parent in parents}
        knownColors = [parent.color for parent in parents]
        colors = itertools.cycle(set(knownColors))
        for change in groups:
            similar = parentsDict.get(change.name, None)
            if change.group is None and similar is not None:
                change.groupId = similar.id
                change.group = similar
            elif change.group is None:
                change.group = Category(
                    name=change.name,
                    parentCategoryId=None,
                    nature=change.nature,
                    color=next(colors, change.color),
                )
                session.add(change.group)
                session.flush()
            elif change.updatedAt < change.group.updatedAt:
                continue
            else:
                change.group.name = change.name
            change.group.updatedAt = datetime.now()
            change.updatedAt = change.group.updatedAt
        session.commit()

    def import_categories(self, session, transactions: List[AkahuApiTransaction]):
        categories = {
            tx.category.id: tx.category
            for tx in transactions
            if tx.category is not None
        }
        importIds = categories.keys()
        query_existing = session.query(AkahuCategory).filter(
            AkahuCategory.akahuId.in_(importIds)
        )
        existing = {e.akahuId: e for e in query_existing.all()}
        changes = [
            (
                existing[category.id].update_from_akahu(category)
                if category.id in existing
                else AkahuCategory.create_from_akahu(category)
            )
            for category in categories.values()
        ]
        session.add_all(changes)
        session.commit()

    def sync_categories(self, session):
        categories = session.query(AkahuCategory).all()
        for change in categories:
            parent = change.group.group if change.group else None
            if change.category is None:
                change.category = Category(
                    parentCategory=parent,
                    name=change.name,
                    nature=parent.nature if parent else change.nature,
                    color=parent.color if parent else change.color,
                )
                session.add(change.group)
                session.flush()
            elif change.updatedAt < change.category.updatedAt:
                continue
            else:
                change.category.name = change.name
            change.category.updatedAt = datetime.now()
            change.updatedAt = change.category.updatedAt
        session.commit()

    def import_transactions(self, session, transactions: List[AkahuApiTransaction]):
        importIds = [tx.id for tx in transactions]
        query_existing = session.query(AkahuTransaction).filter(
            AkahuTransaction.akahuId.in_(importIds)
        )
        existing = {e.akahuId: e for e in query_existing.all()}
        changes = [
            (
                existing[tx.id].update_from_akahu(tx)
                if tx.id in existing
                else AkahuTransaction.create_from_akahu(tx)
            )
            for tx in transactions
        ]
        session.add_all(changes)
        session.commit()

    def sync_transactions(self, session, uncategorized: Category):
        transactions = session.query(AkahuTransaction).all()
        for change in transactions:
            category = change.category.category if change.category else uncategorized
            if change.record is None:
                change.record = Record(
                    accountId=change.account.accountId,
                    categoryId=category.id if category else None,
                    label=change.label,
                    date=change.date,
                    amount=change.amount,
                    isIncome=change.isIncome,
                    tags=change.tags,
                )
                session.add(change.record)
                session.flush()
            elif change.updatedAt < change.record.updatedAt:
                continue
            else:
                change.record.accountId = change.account.accountId
                if category.id:
                    change.record.categoryId = category.id
                change.record.label = change.label
                change.record.date = change.date
                change.record.amount = change.amount
                change.record.isIncome = change.isIncome
                change.record.tags = change.tags
            change.record.updatedAt = datetime.now()
            change.updatedAt = change.record.updatedAt
        session.commit()

    def sync_pending_transactions(
        self,
        session,
        transactions: List[AkahuApiPendingTransaction],
        pending: Category,
    ):
        session.query(Record).filter_by(categoryId=pending.id).delete()
        accountIds = list(set([tx.account for tx in transactions]))
        accounts = (
            session.query(AkahuAccount)
            .filter(AkahuAccount.akahuId.in_(accountIds))
            .all()
        )
        accountsDict = {account.akahuId: account for account in accounts}
        for tx in transactions:
            tags = {"type": tx.type, "imported": "akahu"}
            tagsStr = ", ".join([f"{k}: {v}" for k, v in tags.items()])
            account = accountsDict.get(tx.account)
            if account is None:
                continue
            record = Record(
                accountId=account.accountId,
                categoryId=pending.id,
                label=tx.description,
                date=tx.date,
                amount=math.fabs(tx.amount),
                isIncome=tx.amount > 0,
                isInProgress=True,
                tags=tagsStr,
                updatedAt=tx.updated_at,
            )
            session.add(record)
        session.commit()

    def run(self, start: datetime | None, end: datetime | None):
        with self._session() as session:
            try:
                uncategorized = (
                    session.query(Category).filter_by(name="Uncategorized").first()
                )
                if not uncategorized:
                    uncategorized = Category(
                        name="Uncategorized",
                        nature=Nature.WANT,
                        color="#808080",
                        parentCategoryId=None,
                    )
                    session.add(uncategorized)
                    session.commit()
                pending = session.query(Category).filter_by(name="Pending").first()
                if not pending:
                    pending = Category(
                        name="Pending",
                        nature=Nature.WANT,
                        color="#808080",
                        parentCategoryId=uncategorized.id,
                    )
                    session.add(pending)
                    session.commit()
                accounts = self._akahu.accounts.list()
                self.import_accounts(session, accounts)
                chunks = []
                with click.progressbar(
                    length=100, label=f"Fetching transactions from {start} to {end}..."
                ) as progress:
                    cursor = self._akahu.transactions.list(start, end)
                    while cursor is not None:
                        chunks.append(cursor.items)
                        progress.update(1)
                        cursor = cursor.next()
                    progress.update(100)
                    progress.finish()
                with click.progressbar(
                    chunks, label="Importing transactions"
                ) as progress:
                    for transactions in progress:
                        self.import_groups(session, transactions)
                        self.import_categories(session, transactions)
                        self.import_transactions(session, transactions)
                        progress.update(1)
                    progress.finish()

                click.echo("Fetching pending transactions...")
                pendingTransactions = self._akahu.transactions.pending.list()

                with click.progressbar(
                    length=5, label="Migrating into Bagels"
                ) as progress:
                    progress.label = "Syncing accounts"
                    self.sync_accounts(session)
                    progress.update(1)
                    progress.label = "Syncing groups"
                    self.sync_groups(session)
                    progress.update(1)
                    progress.label = "Syncing categories"
                    self.sync_categories(session)
                    progress.update(1)
                    progress.label = "Syncing transactions"
                    self.sync_transactions(session, uncategorized)
                    progress.update(1)
                    progress.label = "Syncing pending transactions"
                    self.sync_pending_transactions(
                        session, pendingTransactions, pending
                    )
                    progress.update(5)
                    progress.finish()
                print("Import completed successfully!")
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()

    def delete_categories(self):
        with self._session() as session:
            try:
                categories = session.query(AkahuCategory).all()
                groups = session.query(AkahuGroup).all()

                groupIds = [group.groupId for group in groups]
                categoryIds = [category.categoryId for category in categories]

                keep = set(groupIds + categoryIds)
                session.query(Category).filter(Category.id.not_in(keep)).delete()

            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
