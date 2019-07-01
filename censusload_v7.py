import pandas as pd
import censusdata
import psycopg2
from sqlalchemy import create_engine
engine = create_engine('postgresql://naya:urigoren@104.155.40.113:5432/nrich')

class I_censusdata:
    def __init__(self, output_table_name):
        self.output_table_name = output_table_name  # name of table for SQL database
        self.requested_columns = []  # list of columns to be retrieved from census database
        self.output_columns = []  # list of names used to identify columns retrieved from census database
        self.geo = "state" #geo attribute - state used as default
    def re_req_list(self):
        return self.requested_columns
    def re_out_list(self):
        return self.output_columns
    def re_tab_name(self):
        return self.output_table_name
    def re_geo(self):
        return self.geo
    def addallinfo(self, igeo, irequested_columns, ioutput_columns):
        self.geo = igeo
        self.requested_columns=irequested_columns
        self.output_columns =ioutput_columns
    def __repr__(self):
        fprt = "Table Name = " + self.output_table_name + "\n" + "Level = " + self.geo + "\n" + "Source Columns = " + \
               str(self.requested_columns) + "\n" + "Target Columns" + str(self.output_columns)
        return (fprt)

listforcensus={}
with open("censusinputlist.txt") as f: #make sure file is in path
    for line in f:
        lreq_columns = []
        lout_columns = []
        parts = line.rstrip ().split (', ')
        if len(parts)< 2 :
            continue
        output_table_name, geo_attr, req_columns, out_columns = [i.strip() for i in parts]
        lreq_columns = req_columns.strip ("[]").split (',')
        lout_columns = out_columns.strip ("[]").split (',')
        outtable = listforcensus.get (output_table_name, I_censusdata(output_table_name))
        outtable.addallinfo(geo_attr, lreq_columns, lout_columns)
        listforcensus[output_table_name] = outtable
        print(outtable)

req_year = [2017,2012]
req_file = 'acs5'
tables_for_other_years = [0,1,2,4,5,6,7,8]
counter = 0


for dic in listforcensus:

    requested_columns = listforcensus [dic].re_req_list()
    output_columns = listforcensus [dic].re_out_list()
    geo_attr = listforcensus [dic].re_geo()
    output_table_names = listforcensus[dic].re_tab_name ()

    dfdata = censusdata.download(req_file, req_year[0], censusdata.censusgeo([(geo_attr, '*')]),requested_columns)
    dfdata.reset_index (inplace=True)  # this line adds new index and makes old index part of the table
    dfdata.columns = output_columns
    dfdata['survey_file']= req_file
    dfdata['survey_year'] = req_year[0]

    if counter in tables_for_other_years:
        dfdata2 = censusdata.download (req_file, req_year[1], censusdata.censusgeo ([(geo_attr, '*')]),requested_columns)
        dfdata2.reset_index (inplace=True)  # this line adds new index and makes old index part of the table
        dfdata2.columns = output_columns
        dfdata2['survey_file'] = req_file
        dfdata2['survey_year'] = req_year[1]
        dfdata3 = dfdata.append (dfdata2)  # append second dataframe to first
    else:
        dfdata3 = dfdata

    print ("table rows:columns",dfdata3.shape)
    print ("the geo is: ",geo_attr)
    if geo_attr == "state": # added if statement to seperate location column to different columns per geoptype
        dfdata3[['state', 'Sum_lvl',"lvl","stateID"]] = dfdata.location.apply (lambda x: pd.Series (str (x).split (":")))
        print ("the state formatting was done")
    if geo_attr == "county":
        dfdata3[['county', 'Sum_lvl',"lvl","stateID","county_id"]] = dfdata.location.apply (lambda x: pd.Series (str (x).split (":")))
    if geo_attr == 'zip+code+tabulation+area':
        dfdata3[['zcta', 'Sum_lvl',"lvl","zcta_ID"]] = dfdata.location.apply (lambda x: pd.Series (str (x).split (":")))
    dfdata3.drop ('location', axis=1, inplace=True) #drop the location because have problems sending to SQL

    print(dfdata3.shape)
#    dfdata3.to_csv ("C:\\Temp\\output\\df_O_file_" + str (dic)[:15] + ".csv", encoding='utf-8')
#    dfdata3.to_sql(output_table_names, engine, if_exists='replace', index=False)
    counter += 1
    print ("finished round ", counter )
print ("finished! competely!!")
#
# sql = """
# select count(*) from median_earning_by_education
# """
# df = pd.read_sql(sql, con=engine)
# print(df)