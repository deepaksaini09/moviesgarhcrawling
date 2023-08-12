from db_services import dBServices

dBServicesObj = dBServices()


def updateOfBMSURL():
    try:
        bmsStatus = dBServicesObj.updateBMSUrlFromGadgets360()
        dBServicesObj.updateContentsForBMS(bmsStatus)
    except Exception as error:
        print(error)


updateOfBMSURL()
