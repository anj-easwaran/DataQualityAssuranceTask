
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
import pandas as pd

pd.options.mode.chained_assignment = None


JOBS_FILE_PATH = "JKU_jobs.xlsx"
CODE_MAPPING_FILE_PATH = "JKU_code_mapping.xlsx"


class EmploymentProcessor:
    """
    Processing class to transform the raw employment client data into data ready for ingestion
    into our production database.
    """

    def __init__(self, client: str):
        # Import the data files and store them into the class:
        self.client = client

        self.df = pd.read_excel(JOBS_FILE_PATH, sheet_name="Sheet1")
        self.codes = pd.read_excel(CODE_MAPPING_FILE_PATH, sheet_name="action code")

        # We need to turn the codes data into a dictionary for mapping.
        self.action_code_mapping = self.create_action_code_mapping()

        self.job_family_code_mapping = {
            "JKU01": "Real Estate",
            "JKU02": "Litigation",
            "JKU03": "Tax",
            "JKU04": "JKU04" ### need to confirm what this code is
        }

        self.expected_columns = [
            "employee_id",
            "action_type",
            "effective_date",
            "department_data",
            "unique_key",
        ]

    def process(self):
        self.map_values(self.action_code_mapping, "action_type", "ACTION")
        self.map_values(self.job_family_code_mapping, "department_data", "JOB_FAMILY") #this can be made more efficient by creating a list and iterating once, rather than calling the function twice
        #self.map_values(self.job_family_code_mapping, "department_data", "JOB_FAMILY") #remove this so its not duplicated
        self.rename_columns()
        self.generate_unique_key()
        self.create_hire_records()
        self.filter_columns()
        print("--- Data ready for ingestion to database ---")
        self.df.rename(columns={"employee_ID": "Employee ID",
                                "action_type": "Action",
                                "effective_date": "Date",
                                "department_data": "Department",
                                "unique_key" : "Unique Key"
                                }, inplace=True)
        self.save_data()

    def create_action_code_mapping(self) -> dict:
        """Takes the action codes sheet and turns it into a dictionary to use for mapping."""
        action_code_mapping = {}
        if self.codes.empty:
            raise ValueError("codes dataframe is empty - cannot map values")

        action_code_mapping = {
            action: reason
            for action, reason in zip(self.codes["action"], self.codes["action reason"])
        }
        return action_code_mapping

    def map_values(self, code_mapping: dict, col_to_map: str, raw_values: str):
        # """Map values using provided code mapping."""
        # for i in self.df.index:
        #     if self.df[raw_values][i] in code_mapping.keys():
        #         self.df[col_to_map][i] = code_mapping[self.df[raw_values][i]]
        #     else:
        #         self.df[col_to_map][i] = 'Not Mapped'
        self.df[col_to_map] = self.df[raw_values].apply(lambda x: code_mapping.get(x, 'Not Mapped'))  #this is more efficient that for loop (doesnt go through each value but applies to all)
                                                        # lambda takes each value x, and checks if exists in dictionary, if not it returns not mapped
    def create_hire_records(self):
        """Creates hire records for employees missing a 'Hire' action_type."""
        new_records = [] 
        for employee_id in self.df['employee_id'].unique():
            employee_records = self.df[self.df['employee_id'] == employee_id].sort_values(by='effective_date') # Get all records for the current employee
            if not (employee_records['action_type'] == 'Hire').any(): # Check if the employee already has a 'Hire' action type
                earliest_record = employee_records.iloc[0] #use earliest record

                hire_record = { #create hire record for users without 
                    "employee_id": earliest_record["employee_id"],
                    "action_type": "Hire",  # Set the action type to 'Hire'
                    "effective_date": earliest_record["effective_date"],  # Use the earliest date
                    "department_data": earliest_record["department_data"],  # Use department from earliest record
                    "unique_key": f"{self.client}{employee_id}-{earliest_record['effective_date'].strftime('%Y%m%d')}",
                }

                new_records.append(hire_record)

        if new_records: # If there are new hire records to add, append them to the dataframe
            new_df = pd.DataFrame(new_records)
            self.df = pd.concat([self.df, new_df], ignore_index=True) #ignore index clears existing

    def generate_unique_key(self):
        """Generates unique key in the form {client}{id}-{YYYYMMDD}"""
        self.df["effective_date"] = pd.to_datetime(self.df["EFFDT"], format="%m/%d/%Y")
        self.df["unique_key"] = [
            f"{self.client}{emp_id}-{date.strftime('%Y%m%d')}" # changing x to emp_id as x wasnt defined
            for emp_id, date in zip(self.df["employee_id"], self.df["effective_date"])
        ]

    def rename_columns(self):
        """Renames column names."""
        self.df = self.df.rename(columns={"EMPLID": "employee_id"})

    def filter_columns(self):
        self.df = self.df[self.expected_columns]

    def save_data(self):
        """Saves the data we have prepared."""
        self.df = self.df.drop_duplicates().reset_index(drop=True)
        self.df.to_csv("employment.csv", index=False)


def check_all_employees_have_a_hire_record(df: pd.DataFrame) -> None:
    """Checks that all employees present in a DataFrame have a record with action
    type == "Hire".
    """
    all_employees = set(df["employee_id"].unique())
    hire_actions = {"Hire"} 
    hired_employees = set(df.loc[df["action_type"].isin(hire_actions)]["employee_id"].unique())
    unhired_employees = all_employees.difference(hired_employees)
    if unhired_employees:
        print(
            f"There are {len(unhired_employees)} employees missing a hire record!"
        )


employment_processor = EmploymentProcessor(client="jku")
employment_processor.process()
print(employment_processor.df.head()) #this was added so the user can see 5 lines in console

#check_all_employees_have_a_hire_record(employment_processor.df) #we can take this out because we are creating hire records anyway


# If I had more time here’s what I’d change:

# make a model for predicting the missing data eg nearest neighbour/mean median 
# see if there’s any way to make the process more efficient (eg I am used to iterating but pandas has some useful tools like the apply method, which reduces memory usage)
# add more checks for variables like M/F where there is a possibility to be more options. Right now it's very focused on a gender binary.
# could add more logging to show user what is missing/how many , including error handling if file is not found
# method to handle duplicates 
# stripping white space with the strip method 
# reduce memory used where possible (eg using aplpy method was new to me as I usually iterate, but pandas has some functions like apply which would be more efficient)
