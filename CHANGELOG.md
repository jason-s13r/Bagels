# Changelog

## 0.3.9

- Relaxed dependency restrictions

## 0.3.8

- Fixed: Balance not updating when adding records

## 0.3.7

- New! New transfer modal now has autocomplete.
- Fixed: Templates with similar names will now be selected correctly
- Fixed: Crash when adding new account

## 0.3.6

- Added ActualBudget migration
- Fixed datemode styling

## 0.3.4

- Added ability to click on accounts in accounts-mode module
- Fix: Focus removed when switching to persons with empty view

## 0.3.2

- Added error message and user prompt when config error

## 0.3.0

- New! Label, amount and category filters in records module!
- New! Manager page to view your categories and people.
- New! Spending plottings / graphs with estimated spendings!
- New! Budgetting tool for saving money and limiting unnecessary spendings
- Fixed bugs
- Removed barchart in insights.

## 0.2.7

- Fix record module input and loading issues.

## 0.2.6

- Bumped textual to 1.0.0
- Fixed: Account field not required when creating paid split
- Fixed: Beginning balance 0 will pass validation but does not pass non-null constraint

## 0.2.5

- Fixed: Typo in update command

## 0.2.4

- Fixed: Crash when changing offset without app being ready

## 0.2.3

- Added amounts in insight bar legends
- Autocomplete to use templates in label field
- Shift enter to create templates along with a non-transfer record
- Auto update checks
- Fixed: Inconsistent tab behaviors

## 0.2.2

- Fix: remove obscure dependencies

## 0.2.1

- Lock dependencies, rookie mistake

## 0.2.0

- Added formula inputs in amount fields!

## 0.1.16

- Updated bar visuals for non-rounded variation
- WIP Budgets
- WIP Tests

## 0.1.15

- Fix: Deleted categories visible

## 0.1.14

- Fix: Lock textual version and defer upgrade to future

## 0.1.13

- Fix: Income record split value should be subtracted, not added, to the account balance

## 0.1.12

- Added total amount in persons view
- Fix: Transfers not using active date

## 0.1.11

- Add transfers as templates.
- Fix: Crash when go to date in calendar
- Fix: Not creating and using custom config file

## 0.1.10

- View by accounts is now an app action. Records will be filtered as well in that view.
- Fix: Not using active filter date when creating a paid split
- Fix: Crash when entering scientific notation / negative numbers in the amount field
- Fix: New record modal having extra width due to hidden fields
