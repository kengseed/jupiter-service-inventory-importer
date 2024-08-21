import sys
import pandas as pd


def loadSurveyTemplateToDatabase(mode: str, path: str):
    df = pd.read_excel(path)

    for index, row in df.iterrows():
        print(("Debug: {serviceName}").format(serviceName=row["service_name"]))


if len(sys.argv) > 1 and sys.argv[1] != "":
    loadSurveyTemplateToDatabase(sys.argv[1], sys.argv[2])
