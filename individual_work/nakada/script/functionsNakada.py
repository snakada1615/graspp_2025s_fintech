import os
import pandas as pd
from pandas.io.stata import StataReader
import fitz


stateNames1 = [
    'Andhra Pradesh',
    'Arunachal Pradesh',
    'Assam',
    'Bihar',
    'Chhattisgarh',
    'Delhi',
    'Goa',
    'Gujarat',
    'Haryana',
    'Himachal Pradesh',
    'Jharkhand',
    'Karnataka',
    'Kerala',
    'Madhya Pradesh',
    'Maharashtra',
    'Manipur',
    'Meghalaya',
    'Mizoram',
    'Nagaland',
    'Odisha',
    'Punjab',
    'Rajasthan',
    'Sikkim',
    'Tamil Nadu',
    'Telangana',
    'Tripura',
    'Uttar Pradesh',
    'Uttarakhand',
    'West Bengal',
    'A & N Islands',
    'Chandigarh',
    'Dadra & Nagar Haveli',
    'Dadra & Nagar',
    'Dadra & Nagar Haveli and',
    'Jammu & Kashmir',
    'Ladakh',
    'Lakshadweep',
    'Puducherry',
    'All-India',
    'all-India'
]

stateNames2 = [
    'Andhra Pradesh',
    'Arunachal Pradesh',
    'Assam',
    'Bihar',
    'Chhattisgarh',
    'Delhi',
    'Goa',
    'Gujarat',
    'Haryana',
    'Himachal Pradesh',
    'Jharkhand',
    'Karnataka',
    'Kerala',
    'Madhya Pradesh',
    'Maharashtra',
    'Manipur',
    'Meghalaya',
    'Mizoram',
    'Nagaland',
    'Odisha',
    'Punjab',
    'Rajasthan',
    'Sikkim',
    'Tamil Nadu',
    'Telangana',
    'Tripura',
    'Uttar Pradesh',
    'Uttarakhand',
    'West Bengal',
    'Andaman & N. Island',
    'Chandigarh',
    'Dadra & Nagar Haveli & Daman &',
    'Dadra & Nagar Haveli',
    'Dadra & Nagar Haveli and Daman &',
    'Dadra & Nagar Haveli and Daman & Diu',
    'Jammu & Kashmir',
    'Ladakh',
    'Lakshadweep',
    'Puducherry',
    'all-India',
    'All-India'
]


def listVariable(state_dir, return_file):
    # Directory containing your .dta files
    all_vars = []

    for filename in os.listdir(state_dir):
        if filename.endswith('.dta'):
            filepath = os.path.join(state_dir, filename)
            with StataReader(filepath) as reader:
                # Variable labels
                var_labels = reader.variable_labels()
                # Data label (label for the whole dataset)
                data_label = reader.data_label  # May be empty string if not set
                for var, label in var_labels.items():
                    all_vars.append({
                        'file': filename,
                        'data_label': data_label,
                        'variable': var,
                        'variable_label': label
                    })

    # Create DataFrame
    result_df = pd.DataFrame(all_vars)

    # Display or export
    print(result_df)
    result_df.to_csv(return_file, index=False)

    return True

def extractUniqueValue(df, col):
    return df[col].unique()

def extractTextFromPDF(source, pages):
    res = {}
    doc = fitz.open(source)
    for num in pages:
        res[num]=doc.load_page(num).get_text()
    return res

def formatText(myText, stateNames):
    lines = myText.strip().split('\n')

    result = []
    current_state = None
    current_numbers = []
    fieldNumber = 0

    def is_state_name(line):
        res = False
        if line in stateNames:
            res = True
        return res

    def is_number_line(line):
        return all(is_number(part.replace(',', '')) for part in line.split())

    for line in lines:
        line = line.strip()
        if is_state_name(line):
            if current_state and current_numbers:
                result.append([current_state] + current_numbers)
            # initialize state name(current_state) and data(current_numbers)
            current_state = line
            current_numbers = []
        elif is_number_line(line):
            numbers = line.split()
            current_numbers.extend(numbers)

    if current_state and current_numbers:
        result.append([current_state] + current_numbers)

    return result

def is_number(string):
    """Check if a string represents a valid number (including decimals) or is '-'."""
    if string == "-":
        return True
    try:
        float(string)
        return True
    except ValueError:
        return False

def getTableFromPDF(sourceNum, pages):

    def validate_data_structure(data, col_names):
        # """Check if each row in data matches column count."""
        for i, row in enumerate(data):
            if len(row) != len(col_names):
                raise ValueError(
                    f"Row {i} has {len(row)} elements, expected {len(col_names)} columns. "
                    f"Row content: {row}"
                )
        return True

    match sourceNum:
        case 1: 
            source = 'data_original/CAMS Report_October_N.pdf'
            output = 'data_processed/CAMS_page_'
            stateName = stateNames1
        case 2: 
            source = 'data_original/Final_Report_HCES_2023-24L.pdf'
            output = 'data_processed/HCES_page_'
            stateName = stateNames2
        case _: source = "none"

    if source == "none":
        print('select 1 for digital access, 2 for houshold status')
        return False
    else:
        pageContents = extractTextFromPDF(source, pages)
        res = {}
        saveFlag = input('do you want to save output to CSV? (y/n)')
        for i in pages:
            print(i)
            data = formatText(pageContents[i], stateName)
            colCount = len(data[0])
            colNames = [f'col{i}' for i in range(1, colCount+1)]
            # validate data structure
            validate_data_structure(data, colNames)
            df = pd.DataFrame(data, columns=colNames)
            print('page:', i, '----------------------------------------------')
            print(df)
            if (saveFlag == 'y') or (saveFlag == 'Y'):
                fileName = output + str(i) + '.csv'
                df.to_csv(fileName, index=False)
            res[i]=df

        return res

def importWB(database_id, indicator_id, year_from, year_to):
    import requests
    import pandas as pd

    url = (
        f"https://data360api.worldbank.org/data360/data"
        f"?DATABASE_ID={database_id}&INDICATOR={indicator_id}"
        f"&timePeriodFrom={year_from}&timePeriodTo={year_to}&skip=0"
    )
    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)
    data = response.json()

    # The data is under the "value" key
    if "value" in data and data["value"]:
        df = pd.DataFrame(data["value"])
        return df
    else:
        print("No data available for the requested indicator and database.")
        return False

# example of function usage
# mydf_ICTPolicy = importWB("ITU_ICT", "ITU_ICT_G5_DIG_ECON", "2000", "2025")
# mydf_FarmCredit = importWB("FAO_IC", "FAO_IC_23068", "2000", "2025")
# mydf_Value_AgProduction = importWB("FAO_MK", "FAO_MK_22016", "2000", "2025")
# mydf_Value_AgProcessing = importWB("FAO_MK", "FAO_MK_22077", "2000", "2025")
# mydf_Fertilizer = importWB("WB_WDI", "WB_WDI_AG_CON_FERT_ZS", "2000", "2025")
# print(mydf_ICTPolicy.head())

def filterWB(df, countries, colNames, newColNames):
    filtered_df = df[df['REF_AREA'].isin(countries)][colNames]
    filtered_df.columns = newColNames
    return filtered_df

# asianCountries = ['AFG','ARM','AZE','BHR','BGD','BTN','BRN','KHM',
#                   'CHN','GEO','IND','IDN','IRN','IRQ','ISR','JPN','JOR','KAZ','PRK',
#                   'KOR','KWT','KGZ','LAO','LBN','MYS','MDV','MNG','MMR','NPL','OMN',
#                   'PAK','PSE','PHL','QAT','SAU','SGP','LKA','SYR','TJK','THA','TLS',
#                   'TUR','TKM','ARE','UZB','VNM','YEM']

# mydf_ICTPolicy_filtered = filterDF(mydf_ICTPolicy, asianCountries, ["REF_AREA", "TIME_PERIOD", "OBS_VALUE"], ["areaCode", "year", "ICTPolicy"])

def setIndexWB(df, key):
    return df.set_index(key)

# mydf_ICTPolicy_indexed = setIndex(mydf_ICTPolicy_filtered, ["areaCode", "year"])
# mydf_FarmCredit_indexed = setIndex(mydf_FarmCredit_filtered, ["areaCode", "year"])
# mydf_Value_AgProduction_indexed = setIndex(mydf_Value_AgProduction_filtered, ["areaCode", "year"])
# mydf_Value_AgProcessing_indexed = setIndex(mydf_Value_AgProcessing_filtered, ["areaCode", "year"])
# mydf_Fertilizer_indexed = setIndex(mydf_Fertilizer_filtered, ["areaCode", "year"])

def exportCSV(df, fileName):
    output = 'data_processed/' + fileName + '.csv'
    df.to_csv(output)
    return True
