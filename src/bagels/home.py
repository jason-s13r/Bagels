from datetime import datetime, timedelta

from textual.app import ComposeResult
from textual.binding import Binding
from textual.widgets import Label, Static

from bagels.components.modules.accountmode import AccountMode
from bagels.components.modules.datemode import DateMode
from bagels.components.modules.incomemode import IncomeMode
from bagels.components.modules.insights import Insights
from bagels.components.modules.records import Records
from bagels.components.modules.templates import Templates
from bagels.components.modules.welcome import Welcome
from bagels.config import CONFIG
from bagels.managers.accounts import get_accounts_count, get_all_accounts
from bagels.managers.categories import get_categories_count
from bagels.utils.format import format_period_to_readable

# class HomeModeDefaultT(TypedDict):
#     default_value: None
#     default_value_text: str


# class HomeModeT(TypedDict):
#     date: datetime
#     isIncome: bool
#     accountId: HomeModeDefaultT
# TODO: properly type everything


class Home(Static):
    filter = {
        "offset": 0,
        "offset_type": CONFIG.defaults.period,
        "byAccount": False,
    }

    BINDINGS = [
        Binding("left", "dec_offset", "Previous", show=False),
        Binding("right", "inc_offset", "Next", show=False),
        Binding(
            CONFIG.hotkeys.home.cycle_offset_type, "cycle_offset_type", "", show=False
        ),
        Binding(
            CONFIG.hotkeys.home.toggle_use_account,
            "toggle_use_account",
            "Use account",
            show=True,
        ),
        Binding(
            CONFIG.hotkeys.home.toggle_income_mode,
            "toggle_income_mode",
            "Income/Expense",
            show=False,
        ),
        Binding(
            CONFIG.hotkeys.home.select_prev_account,
            "select_prev_account",
            "Select previous account",
            show=False,
        ),
        Binding(
            CONFIG.hotkeys.home.select_next_account,
            "select_next_account",
            "Select next account",
            show=False,
        ),
        Binding("1", "select_template_1", "Template 1", show=False),
        Binding("2", "select_template_2", "Template 2", show=False),
        Binding("3", "select_template_3", "Template 3", show=False),
        Binding("4", "select_template_4", "Template 4", show=False),
        Binding("5", "select_template_5", "Template 5", show=False),
        Binding("6", "select_template_6", "Template 6", show=False),
        Binding("7", "select_template_7", "Template 7", show=False),
        Binding("8", "select_template_8", "Template 8", show=False),
        Binding("9", "select_template_9", "Template 9", show=False),
    ]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs, id="home-page")
        self.isReady = get_accounts_count() and get_categories_count()
        accounts = get_all_accounts()
        self.mode = {
            "isIncome": False,
            "date": datetime.now(),
            "accountId": {
                "default_value": None,
                "default_value_text": "Select account",
            },
        }
        if accounts:
            self.mode["accountId"]["default_value"] = accounts[0].id
            self.mode["accountId"]["default_value_text"] = accounts[0].name
        self.accounts_indices = {"index": 0, "count": len(accounts)}
        self.accounts = accounts
        self.accounts_module = AccountMode(parent=self)
        self.date_mode_module = DateMode(parent=self)
        self.income_mode_module = IncomeMode(parent=self)
        self.record_module = Records(parent=self)
        self.insights_module = Insights(parent=self)
        self.templates_module = Templates(parent=self)

    def on_mount(self) -> None:
        pass

    # -------------- Helpers ------------- #

    def rebuild(self, templates=False) -> None:
        self.insights_module.rebuild()
        self.accounts_module.rebuild()
        self.income_mode_module.rebuild()
        self.date_mode_module.rebuild()
        if self.isReady:
            self.record_module.rebuild()
            if templates:
                self.templates_module.rebuild(reset_state=True)

    def get_filter_label(self) -> str:
        return format_period_to_readable(self.filter)

    def update_filter_label(self, label: Label) -> None:
        string = format_period_to_readable(self.filter)
        label.update(string)

    def get_target_date(self) -> datetime:
        """Returns the date that new records will be based on"""
        offset = self.filter["offset"]
        offset_type = self.filter["offset_type"]
        today = datetime.now()
        if offset == 0:
            return today

        match offset_type:
            case "year":
                return today
            case "month":
                # Get first day of current month
                first_of_month = today.replace(day=1)
                # Add months by adding days and resetting to first of month
                target_date = first_of_month + timedelta(days=32 * offset)
                return target_date.replace(day=1)
            case "week":
                # Get first day of current week
                days_to_first = (
                    today.weekday() - CONFIG.defaults.first_day_of_week
                ) % 7
                first_of_week = today - timedelta(days=days_to_first)
                # Add weeks
                return first_of_week + timedelta(weeks=offset)
            case "day":
                return today + timedelta(days=offset)
        return today

    def _update_date(self) -> None:
        self.mode["date"] = self.get_target_date()

    def set_target_date(self, date: datetime) -> None:
        self.mode["date"] = date

        # we also need to update the filters
        self.filter["offset_type"] = "day"
        offset = (date - datetime.now()).days
        self.filter["offset"] = offset + 1

        # rebuild
        self.rebuild()

    # region Callbacks
    # ------------- Callbacks ------------ #

    def action_dec_offset(self) -> None:
        self.filter["offset"] -= 1
        self._update_date()
        self.rebuild()

    def action_inc_offset(self) -> None:
        if self.filter["offset"] < 0:
            self.filter["offset"] += 1
            self._update_date()
            self.rebuild()
        else:
            self.app.bell()

    def action_cycle_offset_type(self) -> None:
        # Define the cycle order
        cycle_order = ["day", "week", "month", "year"]

        # Get current index
        current_index = cycle_order.index(self.filter["offset_type"])

        next_index = (current_index + 1) % len(cycle_order)

        next_type = cycle_order[next_index]

        # calculate appropriate offset based on the target (mode date)
        match next_type:
            case "week":  # day -> week
                self.filter["offset"] = (
                    self.get_target_date() - datetime.now()
                ).days // 7 + 1
            case "month":  # week -> month
                self.filter["offset"] = (
                    self.get_target_date().year - datetime.now().year
                ) * 12 + (self.get_target_date().month - datetime.now().month)
            case _:
                self.filter["offset"] = 0

        self.filter["offset_type"] = next_type
        self._update_date()

        # Refresh table
        self.rebuild()

    def action_toggle_income_mode(self) -> None:
        self.mode["isIncome"] = not self.mode["isIncome"]
        self.income_mode_module.rebuild()
        self.insights_module.rebuild()

    def _select_account(self, dir: int = 0, id: int = None) -> None:
        if id is not None:
            for index, account in enumerate(self.accounts):
                if account.id == int(id):
                    self.accounts_indices["index"] = index
                    self.mode["accountId"]["default_value"] = account.id
                    self.mode["accountId"]["default_value_text"] = account.name
                    break
        elif self.accounts_indices["count"] > 0:
            new_index = (self.accounts_indices["index"] + dir) % self.accounts_indices[
                "count"
            ]
            self.accounts_indices["index"] = new_index
            self.mode["accountId"]["default_value"] = self.accounts[new_index].id
            self.mode["accountId"]["default_value_text"] = self.accounts[new_index].name

        self.accounts_module.rebuild()
        self.insights_module.rebuild()
        if self.filter["byAccount"]:
            self.record_module.rebuild()

    def action_select_prev_account(self) -> None:
        self._select_account(-1)

    def action_select_next_account(self) -> None:
        self._select_account(1)

    def action_select_account(self, account_id: int) -> None:
        self._select_account(id=account_id)

    def action_toggle_use_account(self) -> None:
        self.filter["byAccount"] = not self.filter["byAccount"]
        self.insights_module.rebuild()
        self.record_module.rebuild()

    # region Templates
    # ------------- Template ------------- #

    def action_select_template_1(self) -> None:
        self.templates_module.select_template(1)

    def action_select_template_2(self) -> None:
        self.templates_module.select_template(2)

    def action_select_template_3(self) -> None:
        self.templates_module.select_template(3)

    def action_select_template_4(self) -> None:
        self.templates_module.select_template(4)

    def action_select_template_5(self) -> None:
        self.templates_module.select_template(5)

    def action_select_template_6(self) -> None:
        self.templates_module.select_template(6)

    def action_select_template_7(self) -> None:
        self.templates_module.select_template(7)

    def action_select_template_8(self) -> None:
        self.templates_module.select_template(8)

    def action_select_template_9(self) -> None:
        self.templates_module.select_template(9)

    # region View
    # --------------- View --------------- #

    def compose(self) -> ComposeResult:
        with Static(classes="home-modules-container"):
            with Static(classes="left"):
                with Static(id="home-top-container"):
                    yield self.accounts_module
                    with Static(id="home-mode-container"):
                        yield self.income_mode_module
                        yield self.date_mode_module
                yield self.insights_module
            with Static(classes="right"):
                if self.isReady:
                    yield self.templates_module
                    yield self.record_module
                else:
                    yield Welcome()
