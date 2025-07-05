from datetime import datetime
from typing import Any, Dict, Tuple

from textual.widget import Widget

from bagels.forms.form import Form, FormField
from bagels.utils.format import parse_formula_expression


def _validate_number(
    value: str, field: FormField, is_float: bool = False
) -> Tuple[bool, str | None]:
    """Validate a number field and return (is_valid, error_message)"""
    if not value:
        if field.is_required:
            return False, "Required", None
        return True, None, value

    # Skip checking if numbers are valid as they are restricted

    # Convert to number for comparisons
    if not is_float:
        num_val = int(value)
    else:
        num_val = parse_formula_expression(value)

    # Check minimum
    if field.min is not None:
        min_val = float(field.min) if is_float else int(field.min)
        if num_val <= min_val:
            return False, f"Must be greater than {field.min}", None

    # Check maximum
    if field.max is not None:
        max_val = float(field.max) if is_float else int(field.max)
        if num_val > max_val:
            return False, f"Must be less than {field.max}", None

    return True, None, num_val


def _validate_date(
    value: str, field: FormField, auto_day: bool = False
) -> Tuple[datetime | None, str | None]:
    """Validate a date field and return (parsed_date, error_message)"""
    if not value or value == "":
        if field.is_required:
            return None, "Required"
        return None, None

    try:
        if auto_day and value.isdigit():
            # Use current month/year if not provided
            this_month = datetime.now().strftime("%m")
            this_year = datetime.now().strftime("%y")
            date = datetime.strptime(f"{value} {this_month} {this_year}", "%d %m %y")
            return date, None
        date = datetime.strptime(value, "%d %m %y")
        return date, None
    except ValueError:
        format_str = "dd (mm) (yy) format." if auto_day else "dd mm yy format"
        return None, f"Must be in {format_str}"


def _validate_autocomplete(
    value: str, held_value: str, field: FormField
) -> Tuple[bool, str | None]:
    """Validate an autocomplete field and return (is_valid, error_message)"""
    if not value and not held_value:
        if field.is_required:
            return False, "Must be selected"
        return True, None

    if not field.options or not field.options.items:
        return True, None

    if field.options.items[0].text:
        # Checks if selected option but user modified input text
        # Find all options with matching text
        matching_items = [item for item in field.options.items if item.text == value]
        if not matching_items:
            # Entered text does not match any option
            return False, "Invalid selection"
        # Check if any of them have the held_value
        if any(str(item.value) == str(held_value) for item in matching_items):
            return True, None
        else:
            # Entered text is not tabbed (selected) or editted a tabbed option
            # Even if entered text matches an item, if it is not tabbed, it is invalid
            return False, "Invalid selection"
    else:
        # if can't find the held_value inside the values, it's invalid
        print(held_value)
        if held_value not in [str(item.value) for item in field.options.items]:
            return False, "Invalid selection"

    return True, None


def validateForm(
    formComponent: Widget, formData: Form
) -> Tuple[Dict[str, Any], Dict[str, str], bool]:
    result = {}
    errors = {}
    isValid = True

    for field in formData.fields:
        fieldKey = field.key
        fieldWidget = formComponent.query_one(f"#field-{fieldKey}")
        fieldValue = (
            fieldWidget.heldValue
            if hasattr(fieldWidget, "heldValue")
            else fieldWidget.value
        )

        error = None

        # print(f"Validating {fieldKey} with value {fieldValue}")

        match field.type:
            case "integer":
                is_valid, error, num_val = _validate_number(fieldValue, field)
                if is_valid and fieldValue is not None and num_val is not None:
                    result[fieldKey] = num_val

            case "number":
                is_valid, error, num_val = _validate_number(
                    fieldValue, field, is_float=True
                )
                if is_valid and fieldValue is not None and num_val is not None:
                    result[fieldKey] = num_val

            case "date":
                date, error = _validate_date(fieldValue, field)
                if date:
                    result[fieldKey] = date

            case "dateAutoDay":
                date, error = _validate_date(fieldValue, field, auto_day=True)
                if date:
                    result[fieldKey] = date

            case "autocomplete":
                if field.autocomplete_selector:
                    is_valid, error = _validate_autocomplete(
                        fieldWidget.value, fieldValue, field
                    )
                    if is_valid and fieldValue:
                        result[fieldKey] = fieldValue
                else:
                    if not fieldWidget.value and field.is_required:
                        error = "Required"
                    else:
                        result[fieldKey] = fieldWidget.value

            case _:
                if not fieldValue and field.is_required:
                    error = "Required"
                else:
                    result[fieldKey] = fieldValue

        if error:
            errors[fieldKey] = error
            isValid = False

    return result, errors, isValid
