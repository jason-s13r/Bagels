import difflib
import math
import itertools
from datetime import datetime
from pathlib import Path
from typing import List
from akahu.models.transaction import (
    Transaction as AkahuApiTransaction,
    PendingTransaction as AkahuApiPendingTransaction,
    TransactionType as AkahuApiTransactionType,
)
from akahu.models.account import Account as AkahuApiAccount
from akahu.client import Client as AkahuClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bagels.locations import database_file
from bagels.models.account import Account
from bagels.models.category import Category, Nature
from bagels.models.record import Record
from bagels.models.split import Split
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

    def import_transactions(
        self,
        session,
        transactions: List[AkahuApiTransaction],
        formattedAccountDict: dict = None,
    ):
        importIds = [tx.id for tx in transactions]
        query_existing = session.query(AkahuTransaction).filter(
            AkahuTransaction.akahuId.in_(importIds)
        )
        existing = {e.akahuId: e for e in query_existing.all()}
        changes = [
            (
                existing[tx.id].update_from_akahu(tx, formattedAccountDict)
                if tx.id in existing
                else AkahuTransaction.create_from_akahu(tx, formattedAccountDict)
            )
            for tx in transactions
        ]
        session.add_all(changes)
        session.commit()

    def sync_transactions(self, session, uncategorized: Category, transfer: Category):
        transactions = session.query(AkahuTransaction).all()
        for change in transactions:
            category = uncategorized
            if change.isTransfer:
                category = transfer
            elif change.category:
                category = change.category.category

            if change.record is None:
                change.record = Record(
                    accountId=change.account.accountId,
                    categoryId=category.id if category else None,
                    label=change.label,
                    date=change.date,
                    amount=change.amount,
                    isIncome=change.isIncome,
                    isTransfer=change.isTransfer,
                    transferToAccountId=(
                        change.transferToAccount.accountId
                        if change.transferToAccount
                        else None
                    ),
                    tags=change.tags,
                )
                session.add(change.record)
                session.flush()
            else:
                change.record.accountId = change.account.accountId
                if change.updatedAt > change.record.updatedAt and category.id:
                    change.record.categoryId = category.id
                change.record.label = change.label
                change.record.date = change.date
                change.record.amount = change.amount
                change.record.isIncome = change.isIncome
                change.record.isTransfer = change.isTransfer
                change.record.transferToAccountId = (
                    change.transferToAccount.accountId
                    if change.transferToAccount
                    else None
                )
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

    def fix_conversions(
        self,
        session,
        transactions: List[AkahuApiTransaction],
        transfer: Category,
        uncategorized: Category,
    ):
        paired = []
        transactions = sorted(transactions, key=lambda tx: math.fabs(tx.amount))
        transactions = list(
            filter(lambda tx: tx.description.startswith("Converted"), transactions)
        )
        targets = list(filter(lambda tx: tx.amount >= 0, transactions))
        sources = list(filter(lambda tx: tx.amount < 0, transactions))

        def matcher(target: AkahuApiTransaction, candidates: List[AkahuApiTransaction]):
            candidates = list(
                filter(lambda tx: tx.account != target.account, candidates)
            )
            candidates = list(filter(lambda tx: tx.date == target.date, candidates))
            candidates = list(filter(lambda tx: tx.id not in paired, candidates))

            candidates = list(
                filter(
                    lambda tx: (
                        tx.meta
                        and tx.meta.conversion
                        and target.description.startswith(
                            f"Converted {tx.meta.conversion.amount:,.2f} {tx.meta.conversion.currency} to {target.amount:,.2f}"
                        )
                    )
                    or (
                        tx.meta
                        and tx.meta.conversion
                        and target.meta
                        and target.meta.conversion
                        and target.description.startswith(
                            f"Converted {target.meta.conversion.amount:,.2f} {target.meta.conversion.currency} from {target.meta.conversion.currency} balance to {tx.meta.conversion.amount:,.2f} {tx.meta.conversion.currency}"
                        )
                    ),
                    candidates,
                )
            )

            candidate = next(iter(candidates), None)

            if candidate:
                paired.append(candidate.id)

            return candidate

        matched = [(target, matcher(target, sources)) for target in targets]

        for target, source in matched:
            if source is None:
                continue
            tx = session.query(AkahuTransaction).filter_by(akahuId=target.id).first()
            rx = session.query(AkahuTransaction).filter_by(akahuId=source.id).first()

            if tx is None or rx is None:
                continue

            if tx.record is not None:
                if (
                    tx.record.categoryId is None
                    or tx.record.categoryId == uncategorized.id
                ):
                    tx.record.categoryId = transfer.id
                    session.add(tx)

            if rx.record is not None:
                if (
                    rx.record.categoryId is None
                    or rx.record.categoryId == uncategorized.id
                ):
                    rx.record.categoryId = transfer.id
                    session.add(rx)

        session.commit()

    def fix_transfers(self, session, transactions: List[AkahuApiPendingTransaction]):
        paired = []
        opposites = {
            AkahuApiTransactionType.DEBIT: AkahuApiTransactionType.CREDIT,
            AkahuApiTransactionType.CREDIT: AkahuApiTransactionType.DEBIT,
        }

        transactions = sorted(transactions, key=lambda tx: math.fabs(tx.amount))
        targets = list(filter(lambda tx: tx.amount >= 0, transactions))
        sources = list(filter(lambda tx: tx.amount < 0, transactions))

        def matcher(target: AkahuApiTransaction, candidates: List[AkahuApiTransaction]):
            candidates = list(
                filter(lambda tx: tx.account != target.account, candidates)
            )
            candidates = list(filter(lambda tx: tx.date == target.date, candidates))
            candidates = list(
                filter(lambda tx: (tx.amount + target.amount) == 0, candidates)
            )
            candidates = list(filter(lambda tx: tx.id not in paired, candidates))

            candidates = sorted(
                candidates,
                key=lambda tx: tx.type == target.type
                or tx.type == opposites[target.type],
                reverse=True,
            )

            candidates = sorted(
                candidates,
                key=lambda tx: difflib.SequenceMatcher(
                    None, tx.description.replace(" TO ", " FROM "), target.description
                ).ratio(),
                reverse=True,
            )

            if len(candidates) > 0:
                print(f"Found {len(candidates)} candidates:")
                print(
                    f"    Target: {target.type} {target.amount} ({target.description})"
                )
                for candidate in candidates:
                    print(
                        f"    Candidate: {candidate.type} {candidate.amount} ({candidate.description}) {candidate.id}"
                    )

            candidate = next(iter(candidates), None)

            if candidate:
                paired.append(candidate.id)

            return candidate

        matched = [(target, matcher(target, sources)) for target in targets]

        for target, source in matched:
            if source is None:
                continue
            tx = session.query(AkahuTransaction).filter_by(akahuId=target.id).first()
            rx = session.query(AkahuTransaction).filter_by(akahuId=source.id).first()

            if tx is None or rx is None:
                continue

            tx.isTransfer = True
            tx.isIncome = False
            tx.transferToAkahuAccountId = source.account
            session.add(tx)

            rx.isTransfer = True
            rx.isIncome = False
            rx.transferToAkahuAccountId = target.account
            session.add(rx)

        session.commit()

    def get_account_balance(self, session, accountId):
        # Initialize balance
        balance = (
            session.query(Account)
            .filter(Account.id == accountId)
            .first()
            .beginningBalance
        )

        # Get all records for this account
        records = session.query(Record).filter(Record.accountId == accountId).all()

        # Calculate balance from records
        for record in records:
            if record.isTransfer:
                # For transfers, subtract full amount (transfers out)
                balance -= record.amount
            elif record.isIncome:
                # For income records, add full amount
                balance += record.amount
            else:
                # For expense records, subtract full amount
                balance -= record.amount

        # Get all records where this account is the transfer destination
        transfer_to_records = (
            session.query(Record)
            .filter(Record.transferToAccountId == accountId, Record.isTransfer == True)  # noqa
            .all()
        )

        # Add transfers into this account
        for record in transfer_to_records:
            balance += record.amount

        # Get all splits where this account is specified
        splits = session.query(Split).filter(Split.accountId == accountId).all()

        # Add paid splits (they represent money coming into this account)
        for split in splits:
            if split.isPaid:
                if split.record.isIncome:
                    balance -= split.amount
                else:
                    balance += split.amount

        return balance

    def fix_account_balance(self, session, accounts: List[AkahuApiAccount]):
        balances = {account.id: account.balance.current for account in accounts}
        akahuIds = balances.keys()
        akahuAccounts: List[AkahuAccount] = (
            session.query(AkahuAccount).filter(AkahuAccount.akahuId.in_(akahuIds)).all()
        )
        idMap = {
            akahuAccount.accountId: akahuAccount.akahuId
            for akahuAccount in akahuAccounts
        }
        beginningBalances = {
            akahuAccount.accountId: akahuAccount.account.beginningBalance
            for akahuAccount in akahuAccounts
        }

        print("")
        for accountId, akahuId in idMap.items():
            beginning = beginningBalances.get(accountId)
            expected = balances.get(akahuId, 0)
            computed = self.get_account_balance(session, accountId)
            rounded_computed = round(computed, 4)
            starting = expected - rounded_computed + beginning
            rounded_starting = round(starting, 4)
            if rounded_starting != 0 and starting != beginning:
                print(f"will fix incorrect beginning balance for account: {accountId}")
                print(f"    balance from Akahu is {expected}")
                print(f"    computed balance is {rounded_computed}")
                print(
                    f"    will set beginning balance to {rounded_starting} (was {beginning})"
                )
                account = session.get(Account, accountId)
                account.beginningBalance = rounded_starting
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
                        parentCategoryId=None,
                    )
                    session.add(pending)
                    session.commit()
                else:
                    pending.parentCategoryId = None
                    session.add(pending)
                    session.commit()
                transfer = session.query(Category).filter_by(name="Transfer").first()
                if not transfer:
                    transfer = Category(
                        name="Transfer",
                        nature=Nature.WANT,
                        color="#808080",
                        parentCategoryId=None,
                    )
                    session.add(transfer)
                    session.commit()
                else:
                    transfer.parentCategoryId = None
                    session.add(transfer)
                    session.commit()
                accounts = self._akahu.accounts.list()
                formattedAccountDict = {
                    account.formatted_account: account.id
                    for account in accounts
                    if account.formatted_account is not None
                }
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
                        self.import_transactions(
                            session, transactions, formattedAccountDict
                        )
                        progress.update(1)
                    progress.finish()

                flattened = list(itertools.chain.from_iterable(chunks))
                click.echo("Pairing transfers...")
                self.fix_transfers(session, flattened)
                self.fix_conversions(session, flattened, transfer, uncategorized)

                click.echo("Fetching pending transactions...")
                pendingTransactions = self._akahu.transactions.pending.list()

                with click.progressbar(
                    length=6, label="Migrating into Bagels"
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
                    self.sync_transactions(session, uncategorized, transfer)
                    progress.update(1)
                    progress.label = "Syncing pending transactions"
                    self.sync_pending_transactions(
                        session, pendingTransactions, pending
                    )
                    progress.update(1)
                    progress.label = "Fix account balances"
                    self.fix_account_balance(session, accounts)
                    progress.update(6)
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
