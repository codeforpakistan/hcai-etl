import os
import time

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pymongo import MongoClient

from utils import *

HCAI_DB = os.getenv("HCAI_DB")
GOOGLE_FILE_URL = os.getenv("GOOGLE_FILE_URL")

client = MongoClient(HCAI_DB)
db = client["hcai"]

while True:

    # fetching hospitals and departments
    hospitals = list(db.hospitals.find({}))
    hospital_and_dept_ward_dict = {}
    for hosp in hospitals:
        hospital_and_dept_ward_dict[str(hosp["_id"])] = hosp["name"]
        for dept in hosp.get("departments", []):
            hospital_and_dept_ward_dict[str(dept["_id"])] = dept["name"]
            for unit in dept.get("units", []):
                hospital_and_dept_ward_dict[str(unit["_id"])] = unit["name"]

    # fetching antibiotics titles
    antibiotics = list(db.antibiotics.find({}))
    antibiotics_dict = {}
    for antib in antibiotics:
        antibiotics_dict[str(antib["_id"])] = antib["title"]

    # fetching all submissions
    submissions = list(db.submissions.find({}))

    # transforming ids into their names
    for sub in submissions:
        try:
            sub["departmentName"] = hospital_and_dept_ward_dict[sub["departmentId"]]
            sub["wardName"] = hospital_and_dept_ward_dict.get(sub["wardId"], "N/A")
            sub["antibioticUsedForProphylaxisName"] = antibiotics_dict.get(
                sub.get("antibioticUsedForProphylaxis", "N/A"), "N/A"
            )
            sub["sensitiveToName"] = antibiotics_dict.get(
                sub.get("sensitiveTo", "N/A"), "N/A"
            )
            sub["resistantToName"] = antibiotics_dict.get(
                sub.get("resistantTo", "N/A"), "N/A"
            )
            sub["intermediateName"] = antibiotics_dict.get(
                sub.get("intermediate", "N/A"), "N/A"
            )
            sub["hospitalName"] = hospital_and_dept_ward_dict[str(sub["hospitalId"])]
        except Exception as e:
            print("Exception", str(e))
            pass

    # creating a dataframe of the submissions and applying some transformations
    submissions_df = pd.DataFrame(submissions).fillna("")
    submissions_df["patientAge"] = submissions_df["patientAge"].apply(
        lambda x: fix_int(x)
    )
    submissions_df["patientAgeBin"] = submissions_df["patientAge"].apply(
        lambda x: get_age_bin_in_text(x)
    )
    submissions_df = submissions_df.loc[submissions_df.dateOfProcedure != ""]
    submissions_df.dateOfProcedure = pd.to_datetime(submissions_df.dateOfProcedure)
    submissions_df["comorbidCondition"] = submissions_df["comorbidCondition"].apply(
        lambda x: ";".join([doc["name"] for doc in x if isinstance(doc, dict)])
    )
    submissions_df["pathogenClassification"] = submissions_df[
        "pathogenClassification"
    ].apply(lambda x: ";".join([doc["name"] for doc in x if isinstance(doc, dict)]))

    # pushing final data frame to google drive sheet
    df = submissions_df
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = Credentials.from_service_account_file(
        "project---1-1554130090913-86b5434d0a64.json", scopes=scopes
    )

    gc = gspread.authorize(credentials)

    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)

    # open a google sheet
    gs = gc.open_by_url(GOOGLE_FILE_URL)
    # select a work sheet from its name
    worksheet1 = gs.worksheet("Sheet1")

    df = df.replace("", "N/A")

    # write to dataframe
    worksheet1.clear()
    set_with_dataframe(
        worksheet=worksheet1,
        dataframe=df,
        include_index=False,
        include_column_header=True,
        resize=False,
    )

    # wait for 60 mins
    time.sleep(60 * 60)
