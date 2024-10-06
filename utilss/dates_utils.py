from datetime import datetime, timedelta
from utilss.constant import holiday_dict

class Dates:
    def __init__(self, end_date, num_days):
        self.end_date = end_date
        self.num_days = num_days
        self.holiday_dates = {datetime.strptime(date, "%d%m%Y") for date in holiday_dict.keys()}
        
    def generate_previous_working_days(self):
        working_days = []
        current_date = self.end_date
        while len(working_days) < self.num_days:
            # Check if the current day is a working day (Monday to Friday) and not a holiday
            if current_date.weekday() < 5 and current_date not in self.holiday_dates:
                working_days.append(current_date.strftime("%d%m%Y"))  # Change format to 'ddmmyyyy'
            # Move to the previous day
            current_date -= timedelta(days=1)
        return working_days