
#created for the purpose of balancing BTG plant data volumes in petrinex

"""
Created on Thurs Jan 19 10:30 am
@author: joshrainbow


edited on Thursday Jan 26 10:30 am
@author: joshrainbow

changes include:
- optimized code for not only balancing BTG plant data but also for balancing any data in the province 
- added in the new balancing factors to complete the list from Bob
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import datetime as dt
import numpy as np


def readData(DataCSV, ACodesCSV):
    # read in the two csv files
    plantData = pd.read_csv(DataCSV)
    plant_AC = pd.read_csv(ACodesCSV)
    print("Data has been read\n")
    
    # drop the variables that we aren't interested in
    plantData = plantData.drop(columns=['ReportingFacilityProvinceState', 'ReportingFacilityType', 'ReportingFacilityIdentifier',
                                          'ReportingFacilitySubTypeDesc','FacilityLegalSubdivision', 'FacilitySection', 'FacilityTownship', 
                                          'FacilityRange', 'FacilityMeridian','FromToIDProvinceState', 'FromToIDType', 'FromToIDIdentifier',
                                          'Hours', 'ProrationProduct', 'ProrationFactor', 'Heat'])
    
    # fixing errors in the volume data
    # Relace the commas with empty string
    plantData['Volume'] = plantData['Volume'].str.replace(',','')

    # Replace the *** with empty string
    plantData['Volume'] = plantData['Volume'].replace(to_replace='\*\*\*', value='0', regex=True)

    # Replace NaN with 0 for Volume
    #plantData['Volume'] = plantData['Volume'].fillna(0)
    plantData = plantData.fillna(0)
    
    # converted volume to float to be able to balance out the volumes via activity
    plantData["Volume"] = pd.to_numeric(plantData["Volume"])
    print("Volume data has been converted to float\n")
    
    # merge the two dataframes
    plantData = pd.merge(plantData, plant_AC, on= 'ActivityID')
    print("Data has been merged\n")
    return plantData


def preprocessColumns(plantData):
    ########################################################################
    # here we remove values that aren't nessecary for the balancing process#
    ########################################################################
    
    # drop the rows where the volume is 0
    plantData = plantData[plantData['Volume'] != 0]
    # drop the rows where the product is sand
    plantData = plantData[plantData['ProductID'] != 'SAND']
    
    
    # drop the rows where the activity is FLARE and the product is ENTGAS#
    ######################################################################
    
    activityNull = 'FLARE'
    productNull = 'ENTGAS'
    activityID = plantData['ActivityID']
    productID = plantData['ProductID']

    # List conditions
    # what were looking for is that if the activityID is FLARE and the productID is ENTGAS, then we want to set the volume to 0
    conditions = [
        (activityID == activityNull) & (productID == productNull), # condition 1
        # other conditions
        (activityID != activityNull) & (productID != productNull), # condition 2
        (activityID == activityNull) & (productID != productNull), # condition 3
        (activityID != activityNull) & (productID == productNull) # condition 4
    ]
    # List of values to return
    choices = [
        0, # if condition 1 is true
        1, # if condition 2 is true
        1, # if condition 3 is true
        1 # if condition 4 is true
    ]
    # create a new column and use np.select to assign values to it using our lists as arguments
    plantData['FLARE&ENTGAS'] = np.select(conditions, choices, default=0)
    plantData = plantData[plantData['FLARE&ENTGAS'] != 0]
    print("Data is ready to be balanced\n")
    return plantData

def balanceData(plantData):
    # create a new column that is the volume multiplied by the factor
    plantData["Balance"] = plantData["Volume"]*plantData["Factor"]
    
    # now we can get all the unique ReportingFacilityID and sum up the balance values
    plantIDs = plantData[["ReportingFacilityID", "Balance"]].copy()
    
    # now we can join and transform each plant ID and sum up the balance values
    plantIDs = plantIDs.join(plantIDs.groupby("ReportingFacilityID").transform(sum).add_prefix('sum_'))
    
    # drop duplicates to get final list of balanced plant ids
    plantIDs = plantIDs.drop(columns=['Balance']).drop_duplicates()
    
    # run a check to see if the sum of the balance values is 0 for each plant ID
    doesNotEqualZero = plantIDs[(plantIDs['sum_Balance'] > 0.05) | (plantIDs['sum_Balance'] < -0.05)]
    Count = len(doesNotEqualZero['ReportingFacilityID'])
    print("There are " + str(Count) + " plants that have not been properly balanced:\n")
    print(doesNotEqualZero)
    
    # merge the unbalanced plant data with the balanced plant data
    plantData = pd.merge(plantData, doesNotEqualZero, on= 'ReportingFacilityID')
    
    #export to csv 
    # we export all Plant Data for plants that haven't been properly balanced
    month = dt.datetime.now().month
    year = dt.datetime.now().year
    plantData.to_csv('plantDataUnbalanced' + str(month) + str(year) + '.csv', index=False)
    print("\nThe Unbalanced Plant Data has been exported to CSV\n")
    print("Check in the folder for the file named plantDataUnbalanced.csv\n")
    return

def main():
    
    # set the name path to the data
    plantDataCSV = "ABPlantDataDec22.CSV"
    activityCodesCSV = "activityCodeFactors.csv"
    
    plantData = readData(plantDataCSV, activityCodesCSV)
    plantData = preprocessColumns(plantData)
    balanceData(plantData)
    return 

main()
    
