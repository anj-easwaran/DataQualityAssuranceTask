"""Employees Data Processing
"""
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
import pandas as pd
import math

pd.options.mode.chained_assignment = None

EMPLOYEES_FILE_1_PATH = "JKU_employees.xlsx"
EMPLOYEES_FILE_2_PATH = "JKU_employees2.xlsx"


class EmployeeProcessor:
    """
    Processing class to transform the raw employees client data into data ready for ingestion
    into our production database.
    """

    def __init__(self, client: str):
        self.client = client
        # Import the data files and store them into the class:
        self.file_one = pd.read_excel(EMPLOYEES_FILE_1_PATH, sheet_name="Sheet1")
        self.file_two = pd.read_excel(EMPLOYEES_FILE_2_PATH, sheet_name="Sheet1")
        self.df = pd.DataFrame()

        self.expected_columns = [
            "employee_id",
            "gender_clean_x",
            "ethnicity",
            "unique_key",
        ]

    def process(self):
        """Main method to run the processing methods."""
        self.df = self.merge_datasets()
        #conflicted_genders = self.reconcile_genders()
        self.column_mapping()
        self.value_mapping()
        self.empty_ethnicity_clean()
        self.create_unique_key()
        self.filter_columns()
        print("--- data ready to be ingested into the database ---")
        self.df.rename(columns={"gender_clean_x": "Gender"}, inplace=True)
        self.save_data()

    def merge_datasets(self) -> pd.DataFrame:
        """Combines the two datafiles sent by the client."""
        return self.file_one.merge(self.file_two, how="inner", on="EMPLID")

    # def reconcile_genders(self):
    #     for i in self.df.index:
    #         if self.df.at[i, "GENDER_x"] != self.df.at[i, "GENDER_y"]:
    #             self.df.at[i, "GENDER_x"] = "Conflicting genders"
        
        # Count and print the number of conflicts found
        #num_conflicts = (self.df["GENDER_x"] == "Conflicting genders").sum()
        #print(f"Warning: {num_conflicts} employees have conflicting entries for gender.")

        # Optionally return the rows with conflicts
        #return self.df[self.df["GENDER_x"] == "Conflicting genders"]
            
        #     x or y or None for x, y in zip(self.df["GENDER_x"], self.df["GENDER_y"])
        # ]
        # conflicted = self.df[self.df["GENDER_x"] != self.df["GENDER_y"]]
        # self.df.loc[self.df["GENDER_x"] != self.df["GENDER_y"], "GENDER_x"] = "Conflicted"
        # print(type(conflicted))
        # print(
        #     f"Warning: {conflicted.shape[0]} employees have conflicting entries for gender."
        # )
        # return conflicted

    def column_mapping(self):
        """Renames the columns to match what is needed in the production database."""
        column_mapping = {
            "EMPLID": "employee_id",
            "GENDER_x": "gender_clean_x",
            "GENDER_y": "gender_clean_y",
            "ETHNIC_DESC": "ethnicity",
        }
        self.df = self.df.rename(columns=column_mapping)

    def value_mapping(self):
        """Map gender values to their full forms and handle conflicts."""

        def map_gender(row):
            gender_x = row['gender_clean_x']
            gender_y = row['gender_clean_y']

            if (pd.isna(gender_x) or gender_x == "") and (pd.isna(gender_y) or gender_y == ""):
                return "Not Specified" #If both are empty, return 'Not Specified'
            
            if gender_x == gender_y:
                if gender_x == "F":
                    return "female"
                elif gender_x == "M":
                    return "male" #If both are the same, map "F" to "female" and "M" to "male"
                else:
                    return gender_x  #new line if not F or M
            
            if pd.isna(gender_x) or gender_x == "":  #If gender_x is empty, take the value from gender_y
                return gender_y

            if gender_x != gender_y: #if they are different eg F and M
                return "Conflicting"

        # Apply the mapping function to each row of the DataFrame
        self.df['gender_clean_x'] = self.df.apply(map_gender, axis=1)

    def create_unique_key(self):
        """Creates the unique key column of the form {CLIENT}{employee_id}."""
        self.df["unique_key"] = [f"{self.client}{x}" for x in self.df["employee_id"]]

    def filter_columns(self):
        """Filters the columns of the dataframe so we only have what is needed in the production database."""
        self.df = self.df[self.expected_columns]

    def save_data(self):
        """Saves the data we have prepared."""
        self.df.to_csv("employees.csv", index=False)

    def empty_ethnicity_clean(self):
        self.df["ethnicity"] = self.df["ethnicity"].fillna("Not Specified")

    def create_filtered_dataset(self): #this was created to create a separate file with missing/clashing data
        filtered_df = self.df.loc[
            (self.df["Gender"].isin(["Not Specified", "Conflicting"])) | # Filter rows where gender_clean_x is 'Not Specified' or 'Conflicting', or ethnicity is 'Not Specified'
            (self.df["ethnicity"] == "Not Specified")
        ]   
        filtered_df.to_csv("employees_unspecified_data.csv", index=False)
        return filtered_df


# Run the processing class
employee_processor = EmployeeProcessor(client="jku")
employee_processor.process()
filtered_employees = employee_processor.create_filtered_dataset()

print(employee_processor.df.head())
print(print(" --- this dataset contains users with unspecified gender or ethnicity ---"), filtered_employees.head())
