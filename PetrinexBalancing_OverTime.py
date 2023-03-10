
#created for the purpose of balancing AB plant data volumes in petrinex

"""
Created on Thurs Jan 19 10:30 am
@author: joshrainbow

changes include:

- optimized code for not only balancing AB plant data but also for balancing any data in the province 
- added in the new balancing factors to complete the list

edited on Friday Feb 10 10:30 am
@author: joshrainbow

change include:
- tranfering the code to functions that can be called from the main program
- if changes need to be made to the code, they can be made in the preprocessColumns function and not in the main program

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
    
    # merge the two dataframes
    plantData = pd.merge(plantData, plant_AC, on= 'ActivityID')
    print("Data has been merged\n")
    
    return plantData


def preprocessColumns(plantData):
    ###########################
    #clean and format the data#
    ###########################
    
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
    

    
    ########################################################################
    # here we remove values that aren't nessecary for the balancing process#
    ########################################################################
    
    # drop the rows where the volume is 0
    plantData = plantData[plantData['Volume'] != 0]
    print("dropped rows where volume is 0\n")
    
    # drop the rows where the product is sand - **SAND DOES NOT BALANCE**
    plantData = plantData[plantData['ProductID'] != 'SAND']
    print("dropped rows where product is sand\n")
    print("Data is ready to be balanced\n")
    return plantData
    
    


def balanceData(plantData):
    # create a new column that is the volume multiplied by the factor
    plantData["Balance"] = plantData["Volume"]*plantData["Factor"]
    
    # now we can get all the unique ReportingFacilityID and sum up the balance values
    plantIDs = plantData[["ReportingFacilityID", "Balance"]].copy()
    
    # now we can join and transform each plant ID and sum up the balance values
    plantIDs = plantIDs.join(plantIDs.groupby("ReportingFacilityID").transform(sum).add_prefix('sum'))
    
    # drop balance column and duplicates to get final list of balanced plant ids
    plantIDs = plantIDs.drop(columns=['Balance']).drop_duplicates()
    
    # run a check to see if the sum of the balance values is 0 for each plant ID
    doesNotEqualZero = plantIDs[(plantIDs['sumBalance'] > 0.05) | (plantIDs['sumBalance'] < -0.05)]
    Count = len(doesNotEqualZero['ReportingFacilityID'])
    doesNotEqualZero = doesNotEqualZero.sort_values(by=['sumBalance'])
    print("There are " + str(Count) + " plants that have not been properly balanced:\n")
    print(doesNotEqualZero)
    
    # merge the unbalanced plant data with the balanced plant data
    plantData = pd.merge(plantData, doesNotEqualZero, on= 'ReportingFacilityID')
    
    return plantData


def rebalanceData(plantData):
    ######################################################################
    # drop the rows where the activity is FLARE and the product is ENTGAS#
    ######################################################################
    
    # maybe throw a hash map here and then we can group everything in pairs and have it loop through each pair
    ############ iterate through each activity and product pair and create a new column that is 0 if the activity and product match the pair and 1 if they don't
    activityList = ['FLARE']
    productList = ['ENTGAS']
    activityID = plantData['ActivityID']
    productID = plantData['ProductID']
    counter = 0
    
    # loop through each activity and product pair
    for (activity, product) in zip(activityList, productList):
        # List conditions
        conditions = [
            # conditions for combination 'activity * product'
            (activityID == activity) & (productID == product), # condition 1
            # other conditions
            (activityID != activity) & (productID != product), # condition 2
            (activityID == activity) & (productID != product), # condition 3
            (activityID != activity) & (productID == product) # condition 4
        ]
        # List of values to return
        choices = [
            0, # if condition 1 is true
            1, # if condition 2 is true
            1, # if condition 3 is true
            1 # if condition 4 is true
        ]
        # create a new column and use np.select to assign values to it using our lists as arguments
        header = 'NullCombination' + str(counter)
        plantData[header] = np.select(conditions, choices, default=0)
        print("\n" + header + " has been created\n")
        counter += 1
        
    # remove the rows where null combination is 0
    for i in range(counter):
        header = 'NullCombination' + str(i)
        plantData = plantData[plantData[header] != 0]
    # drop Balance and sumBalance columns to rebalance
    plantData = plantData.drop(columns=['Balance', 'sumBalance'])
    
    return plantData


def exportData(plantData):
    #export to csv 
    # we export all Plant Data for plants that haven't been properly balanced
    month = dt.datetime.now().month
    year = dt.datetime.now().year
    date = str(month) + str(year)
    plantData.to_csv('plantDataUnbalanced' + date + '.csv', index=False)
    print("\nThe Unbalanced Plant Data has been exported to CSV\n")
    print("Check in the folder for the file named plantDataUnbalanced" + date + ".csv\n")
    return


''' 
now bob wants to be able to select a certain time frame and example june 2015 - jan 2022 and 
then loop through all related csv

specifically can we do this for one or more faclilities

'''

# first we need two nests loops one for the months and one for the years
def monthYearIterator(startMonth, startYear, endMonth, endYear):
    month = startMonth
    year = startYear
    while year < endYear or (year == endYear and month <= endMonth):
        yield year, month
        month += 1
        if month == 13:
            month = 1
            year += 1


def main():
    
    sMonth = int(input("Enter the start month: "))
    sYear = int(input("Enter the start year: "))
    eMonth = int(input("Enter the end month: "))
    eYear = int(input("Enter the end year: "))
    csvOutputList = []
    
    # now we need to loop through the months and years
    for y, m in monthYearIterator(sMonth, sYear, eMonth, eYear):
        # then we need to create a string that is the month and year
        if len(str(m)) == 1:
            date = str(y) + "-0" + str(m)
        else:
            date = str(y) + "-" + str(m)
        # then we need to create a string that is the name of the csv file
        plantDataCSV = "Vol_" + date + "-AB.CSV"
        
        # set the name path to the data
        activityCodesCSV = "activityCodeFactors.csv"
        
        plantData = readData(plantDataCSV, activityCodesCSV)
        plantDataPP = preprocessColumns(plantData)
        plantDataB = balanceData(plantDataPP)
        plantDataRB = rebalanceData(plantDataB)
        plantDataB = balanceData(plantDataRB)
        # variable to store csv file name to add to list
        csvOutput = "plantDataUnbalanced" + date + ".csv"
        plantDataB.to_csv(csvOutput, index=False)
        
        # csv list to store all the csv file names
        csvOutputList.append(csvOutput)
        
    
    # concat all the csv files into one master csv file
    df_concat = pd.concat([pd.read_csv(f) for f in csvOutputList ], ignore_index=True)
    df_concat.to_csv("plantDataUnbalancedMaster.csv", index=False)
        #exportData(plantDataB)
    return 


main()
    
