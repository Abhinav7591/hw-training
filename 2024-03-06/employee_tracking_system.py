import json
from datetime import datetime
from pymongo import MongoClient

class EmployeeTaskTracker:
    client = MongoClient('localhost', 27017) 
    db = client['employee_tasks']  
    collection = db['tasks']  

    def __init__(self, emp_name, emp_id):
        self.emp_name = emp_name
        self.emp_id = emp_id
        
        self.login_time = None
        self.logout_time = None
        self.tasks = []

    def login(self):
        self.login_time = datetime.now()

    def logout(self):
        self.logout_time = datetime.now()
        self._create_daily_json_file()
        self._save_to_mongodb()

    def add_task(self, task_title, task_description):
        task = {
            "task_title": task_title,
            "task_description": task_description,
            "start_time": datetime.now().strftime('%Y-%m-%d %H:%M'),
            "end_time": None,
            "task_success": None
        }
        self.tasks.append(task)

    def end_task(self, task_index):
        if task_index < len(self.tasks):
            self.tasks[task_index]["end_time"] = datetime.now().strftime('%Y-%m-%d %H:%M')
            self.tasks[task_index]["task_success"] = True

    def _create_daily_json_file(self):
        data = {
            "emp_name": self.emp_name,
            "emp_id": self.emp_id,
            "login_time": self.login_time.strftime('%Y-%m-%d %H:%M'),
            "logout_time": self.logout_time.strftime('%Y-%m-%d %H:%M'),
            "tasks": self.tasks
        }
        file_name = f"{datetime.now().strftime('%Y-%m-%d')}_{self.emp_name.replace(' ', '_')}.json"
        with open(file_name, 'w') as file:
            json.dump(data, file, indent=4)

    def _save_to_mongodb(self):

        collection_name = f"{self.emp_name.replace(' ', '_')}_{self.emp_id}"

        collection = self.db[collection_name]

        data = {
            "emp_name": self.emp_name,
            "emp_id": self.emp_id,
            "login_time": self.login_time,
            "logout_time": self.logout_time,
            "tasks": self.tasks
        }
        collection.insert_one(data)


if __name__ == "__main__":
    employee1 = EmployeeTaskTracker("Adarsh", 1)
    employee1.login()
    employee1.add_task("Task 1", "completed the pending task")
    employee1.add_task("Task 2", "do something")
    employee1.end_task(0)
    employee1.end_task(1)
    employee1.logout()
