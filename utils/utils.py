import yaml
import datetime


def get_yml(path):
    with open(path, 'r') as f:
        creds = yaml.safe_load(f)
        return creds


def get_quarter_start(date):
    year = date.year
    quarter_number = (date.month - 1) // 3 + 1
    month = (quarter_number - 1) * 3 + 1
    return datetime.date(year, month, 1), quarter_number


