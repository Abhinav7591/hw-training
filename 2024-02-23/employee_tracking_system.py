import json
from datetime import datetime

class EmployeeTaskTracker:
    employee_tasklist = []

    def __init__(self, emp_name, emp_id):
        self.emp_name = emp_name
        self.emp_id = emp_id
        
        self.login_time = None
        self.logout_time = None
        self.tasks = []

    def login(self):
        self.login_time = datetime.now()
