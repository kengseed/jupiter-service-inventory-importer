import sys
import pandas as pd
import configparser
from database.jupiter_service_inventory_repository import ServiceInventoryRepository


def loadSurveyTemplateToDatabase(mode: str, mainPath: str, secondaryPath: str):
    df = pd.read_excel(mainPath)
    repository = ServiceInventoryRepository()

    print(("Loading survey template ({mode}) to database...").format(mode=mode))

    match mode:
        case "online-provider-open":
            repository.loadServiceCatalogOpen(df.iterrows())
        case "online-provider-mainframe":
            repository.loadServiceCatalogMainframe(df.iterrows())
        case "online-dependency":
            mainframeDf = pd.read_excel(secondaryPath)
            repository.loadInterfaceDependency(df.iterrows(), mainframeDf.iterrows())
        case "batch-dependency":
            mainframeDf = pd.read_excel(secondaryPath)
            repository.loadBatchDependency(df.iterrows(), mainframeDf.iterrows())
        case "online-integration-hub-mapping":
            repository.loadIntegrationServiceToHubMapping(df.iterrows())

    print(("Loaded survey template ({mode}) to database!!").format(mode=mode))


if len(sys.argv) > 1 and sys.argv[1] != "":
    loadSurveyTemplateToDatabase(sys.argv[1], sys.argv[2], sys.argv[3])
