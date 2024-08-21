import sys
import pandas as pd
import database.jupiter_service_inventory_repository as repository


def loadSurveyTemplateToDatabase(mode: str, path: str):
    df = pd.read_excel(path)

    print(("Loading survey template ({mode}) to database...").format(mode=mode))

    match mode:
        case "online-provider-open":
            repository.loadServiceCatalogOpen(df.iterrows())
        case "online-provider-mainframe":
            repository.loadServiceCatalogMainframe(df.iterrows())
        case "online-dependency-open":
            repository.loadInterfaceDependencyOpen(df.iterrows())
        case "online-dependency-mainframe":
            repository.loadInterfaceDependencyMainframe(df.iterrows())
        case "batch-open":
            repository.loadBatchOpen(df.iterrows())
        case "batch-mainframe":
            repository.loadBatchMainframe(df.iterrows())

    print(("Loaded survey template ({mode}) to database!!").format(mode=mode))


if len(sys.argv) > 1 and sys.argv[1] != "":
    loadSurveyTemplateToDatabase(sys.argv[1], sys.argv[2])
