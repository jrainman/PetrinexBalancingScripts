#created for the purpose of balancing BTG plant data volumes in petrinex

"""
Created on Thurs Jan 19 10:30 am
@author: joshrainbow

changes include:
- optimized code for not only balancing BTG plant data but also for balancing any data in the province 
- added in the new balancing factors to complete the list from Bob

edited on Friday Feb 10 10:30 am
@author: joshrainbow

change include:
- tranfering the code to functions that can be called from the main program
- if changes need to be made to the code, they can be made in the preprocessColumns function and not in the main program

"""


import pandas as pd
import datetime as dt
import numpy as np
import os

def readData(DataCSV, ACodesCSV, facilityList):
    # read in the two csv files
    plantData = pd.read_csv(DataCSV, usecols=['ProductionMonth', 'OperatorName', 'ReportingFacilityID','ReportingFacilityType', 
                                              'ReportingFacilitySubType', 'ReportingFacilityID','ReportingFacilityName',
                                              'ReportingFacilitySubTypeDesc', 'ReportingFacilityLocation', 'FacilityLegalSubdivision',
                                              'FacilitySection','FacilityTownship', 'FacilityRange', 'FacilityMeridian',
                                               'ProductID','Volume','Energy','ActivityID'])
                                              
    plant_AC = pd.read_csv(ACodesCSV)
    
    facilityList = facilityList
    plantData = plantData[plantData["ReportingFacilityID"].isin(facilityList)]
    
    plantData["Volume"] = plantData["Volume"].fillna(0)
    plantData = plantData.fillna("NaN")
    
    # merge the two dataframes
    plantData = pd.merge(plantData, plant_AC, on= 'ActivityID', how = 'left')
    plantData["Factor"] = plantData["Factor"].fillna(0)
    return plantData

def preprocessColumns(plantData):
    ###########################
    #clean and format the data#
    ###########################
    #set the volume column to string **edge case**
    plantData['Volume'] = plantData['Volume'].astype(str)
    
    # fixing errors in the volume data
    # Relace the commas with empty string
    plantData['Volume'] = plantData['Volume'].str.replace(',','')

    # Replace the *** with empty string
    plantData['Volume'] = plantData['Volume'].replace(to_replace='\*\*\*', value='0', regex=True)
    
    # Replace the blanks with 0
    plantData["Volume"] = plantData["Volume"].fillna(0)
    
    # converted volume to float to be able to balance out the volumes via activity
    plantData["Volume"] = pd.to_numeric(plantData["Volume"])
    
    #############################################################################
    # here we set to null values that aren't nessecary for the balancing process#
    #############################################################################
    
    activityList = ['FLARE']
    productList = ['ENTGAS']
    activityID = plantData['ActivityID']
    productID = plantData['ProductID']
    counter = 0
    
    # loop through each activity and product pair
    for (activity, product) in zip(activityList, productList):
        # List conditions
        conditions = [
            (productID == "SAND"), # condition for sand
            # conditions for combination 'activity * product'
            (activityID == activity) & (productID == product), # condition 1
            # other conditions
            (activityID != activity) & (productID != product), # condition 2
            (activityID == activity) & (productID != product), # condition 3
            (activityID != activity) & (productID == product) # condition 4
        ]
        # List of values to return
        choices = [
            0, # if condition SAND is true
            0, # if condition 1 is true
            1, # if condition 2 is true
            1, # if condition 3 is true
            1 # if condition 4 is true
        ]
        # create a new column and use np.select to assign values to it using our lists as arguments
        header = 'NullCombination' + str(counter)
        plantData[header] = np.select(conditions, choices, default=0)
        counter += 1
    
    nullCombinationList = []
    # remove the rows where null combination is 0
    for i in range(counter):
        header = 'NullCombination' + str(i)
        nullCombinationList.append(header) 
    # create a new column that is the product of all the null combinations
    plantData['nullFactor'] = plantData.loc[:, nullCombinationList].prod(axis=1)
    plantData = plantData.drop(nullCombinationList, axis=1)
    return plantData
   
def balanceData(plantData):
    # create a new column that is the volume multiplied by the factor
    plantData["Balance"] = plantData["Volume"] * plantData["Factor"] * plantData["nullFactor"]
    
    # drop the variables that we aren't interested in
    plantData = plantData.drop(columns=['nullFactor'])
    
    # now we can get all the unique ReportingFacilityID and sum up the balance values
    plantIDs = plantData[["ReportingFacilityID", "Balance"]].copy()
    
    # now we can join and transform each plant ID and sum up the balance values
    plantIDs = plantIDs.join(plantIDs.groupby("ReportingFacilityID").transform(sum).add_prefix('sum'))
    
    # drop balance column and duplicates to get final list of balanced plant ids
    plantIDs = plantIDs.drop(columns=['Balance']).drop_duplicates()
    
    ##############################################################################
    # run a check to see if the sum of the balance values is 0 for each plant ID #
    ##############################################################################
    
    # if the sum is not 0, then the plant ID is unbalanced
    plantsUnbalanced = plantIDs[(plantIDs['sumBalance'] > 0.05) | (plantIDs['sumBalance'] < -0.05)]
    plantsUnbalanced['Unbalanced/Balanced'] = "Unbalanced"
    countUnbalanced = len(plantsUnbalanced['ReportingFacilityID'])
    
    # if the sum is 0, then the plant ID is balanced
    plantsBalanced = plantIDs[(plantIDs['sumBalance'] < 0.05) & (plantIDs['sumBalance'] > -0.05)]
    plantsBalanced['Unbalanced/Balanced'] = "Balanced"
    countBalanced = len(plantsBalanced['ReportingFacilityID'])
    countPlants = countUnbalanced + countBalanced
    
    # basic grammar nested loop 
    if countPlants == 1: # if one plant it is either balanced or not
        print(f"\nThere is {countPlants} plant in total, \n")
        # if statment to print out the number of unbalanced plants if there are any
        if countUnbalanced == 0:
            print("and the plant has been properly balanced.\n")
        else:
            print("and the plant has not been properly balanced\n")          
    # if there is more than one plant here is the text
    else: 
        print(f"\nThere are {countPlants} plants in total, \n")
        # if statment to print out the number of unbalanced plants if there are any
        if countUnbalanced == 0:
            print("and all plants have been properly balanced.\n")
        else:
            print(f"and {countUnbalanced} plants have not been properly balanced.\n")
            print(f"Whereas {countBalanced} plants have been properly balanced.\n")
    
    # merge the unbalanced plant data with the balanced plant data
    plantDataUnbalanced = pd.merge(plantData, plantsUnbalanced, on= 'ReportingFacilityID')
    plantDataBalanced = pd.merge(plantData, plantsBalanced, on= 'ReportingFacilityID')
    plantData = pd.concat([plantDataBalanced, plantDataUnbalanced], ignore_index=True)
    plantData = plantData.sort_values(by=['sumBalance'])

    # round the Sum and balance values
    decimals = 2 # number of decimal places to round to
    plantData = plantData.round({'sumBalance': decimals, 'Balance': decimals}) 
    return plantData

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
    # activity code factors path
    activityCodesCSV = "activityCodeFactors.csv"
    
    #################################################################################
    ############ bounds for month and year ##########################################
    #################################################################################
    # date bound for year set to data that we have in the current year
    yearBound = dt.datetime.now().year
    monthBound = dt.datetime.now().month
    # subtract 2 to deal with reporting lag
    monthBound = monthBound - 2
    #################################################################################
    #################################################################################
    facilityList = []
    while True:
        # get the facility ID from the user
        facilityID = input("Enter the facility ID you would like to balance, when done press enter twice: ").upper()
        # if the user enters '' then break out of the loop
        # if the user enters dupicates keep asking them to loop
        while facilityID in facilityList:
            print("You have already entered this facility ID\n")
            facilityID = input("Enter the facility ID you would like to balance, when done press enter twice: ").upper()
        if facilityID == '':
            break
        else:
            facilityList.append(facilityID)
    #################################################################################
    # flag to check if plant is in database
    if len(str(monthBound)) == 1:
        date = f"{yearBound}-0{monthBound}"
    else:
        date = f"{yearBound}-{monthBound}"
    plantDataCSV = f"Vol_{date}-AB.CSV"
    plantData = readData(plantDataCSV, activityCodesCSV, facilityList)
    facilityIDList = plantData['ReportingFacilityID']
    if len(facilityIDList) == 0:
        print(f"There is no data for {facilityList} in the database\n")
        exit()
    #################################################################################
    csvOutputList = []
    # loop to run all functions over the desired date range
    # set start year and month to 1 and 2015 respectively
    # set end year and month to the current month and year
    for y, m in monthYearIterator(1, 2015, monthBound, yearBound):
        # then we need to create a string that is the month and year
        if len(str(m)) == 1:
            date = f"{y}-0{m}"
        else:
            date = f"{y}-{m}"
        # then we need to create a string that is the name of the csv file
        plantDataCSV = f"Vol_{date}-AB.CSV"
        
        # functions in the program
        plantData = readData(plantDataCSV, activityCodesCSV, facilityList)
        facilityIDList = plantData['ReportingFacilityID']
        if len(facilityIDList) == 0:
            print(f"There is no data for the month of {date} for the following plants you selected:\n")
        else:
            plantDataPP = preprocessColumns(plantData)
            plantDataB = balanceData(plantDataPP)
            csvOutput = "PlantDataBalancedMaster" + date + ".csv"
            plantDataB.to_csv(csvOutput, index=False)
            csvOutputList.append(csvOutput)
    
    print("Combining all the months into one csv file...")
    df_concat = pd.concat([pd.read_csv(f) for f in csvOutputList], ignore_index=True)
    x = df_concat.info()
    print(x)
    fileNameEXP = "PlantDataBalancedMaster.csv"
    df_concat.to_csv(fileNameEXP, index=False)
    
    # remove all the csv files that were created
    for files in csvOutputList:
        os.remove(files)
    print(f"The csv file has been created and saved as {fileNameEXP}, for the following plants you selected:\n")
    return 
    
main()