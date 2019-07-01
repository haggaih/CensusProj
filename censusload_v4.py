import pandas as pd
import censusdata
import psycopg2
from sqlalchemy import create_engine
engine = create_engine('postgresql://naya:urigoren@104.155.40.113:5432/nrich')

geo_attr = ['state','state','state','county','county','zip+code+tabulation+area','state','state','state','state',]
requested_columns = [['GEO_ID','B20004_001E', 'B20004_002E', 'B20004_003E', 'B20004_004E', 'B20004_005E','B20004_006E'],
                     ['GEO_ID','B21001_001E', 'B21001_002E', 'B21001_003E', 'B21001_004E', 'B21001_005E','B21001_006E',
                      'B21001_022E','B21001_023E','B21001_024E'],
                     ['GEO_ID','B22003_001E', 'B22003_002E', 'B22003_003E','B22003_004E','B22003_005E','B22003_006E','B22003_007E'],
                     ['GEO_ID','B05002_001E', 'B05002_002E','B05002_013E','B05002_014E','B05002_021E'],
                     ['GEO_ID','B11001_001E', 'B11001_002E','B11001_007E'],
                     ['GEO_ID','B11001_001E', 'B11001_002E','B11001_007E'],
                     ['GEO_ID','B13002_001E','B13002_002E','B13002_011E','B13002A_001E','B13002A_002E','B13002A_005E',
                      'B13002B_001E','B13002B_002E','B13002B_005E','B13002C_001E','B13002C_002E','B13002C_005E','B13002D_001E',
                      'B13002D_002E','B13002D_005E','B13002E_001E','B13002E_002E','B13002E_005E','B13002F_001E',
                      'B13002F_002E','B13002F_005E','B13002G_001E','B13002G_002E','B13002G_005E','B13002H_001E','B13002H_002E',
                      'B13002H_005E','B13002I_001E','B13002I_002E','B13002I_005E'],
                     ['GEO_ID','B17003_001E','B17003_002E','B17003_003E','B17003_004E','B17003_005E','B17003_006E','B17003_007E',
                      'B17003_008E','B17003_009E','B17003_010E','B17003_011E','B17003_012E'],
                     ['GEO_ID','B15002_001E','B15002_002E','B15002_003E','B15002_004E','B15002_005E','B15002_006E',
                      'B15002_007E','B15002_008E','B15002_009E','B15002_010E','B15002_011E','B15002_012E',
                      'B15002_013E','B15002_014E','B15002_015E','B15002_016E','B15002_017E','B15002_018E',
                      'B15002_019E','B15002_020E','B15002_021E','B15002_022E','B15002_023E','B15002_024E',
                      'B15002_025E','B15002_026E','B15002_027E','B15002_028E','B15002_029E','B15002_030E',
                      'B15002_031E','B15002_032E','B15002_033E','B15002_034E','B15002_035E'],
                     ['GEO_ID','B25034_001E','B25034_002E','B25034_003E','B25034_004E','B25034_005E','B25034_006E','B25034_007E',
                      'B25034_008E','B25034_009E','B25034_010E','B25034_011E']]

output_columns = [['location','geo_id','m_e_total','m_e_lt_high school','m_e_high_school_graduate','m_e_college_degree', 'm_e_bachelore_degree',
                   'm_e_professional_degree'],
                  ['location','geo_id','total','total_veterans','total_not_vaternas','total_male', 'total_male_veterans','total_male_not_veternas',
                    'total_female','total_female_veterans','total_female_not_veterns'],
                  ['location','geo_id','total','received_total','received_below_poverty_level','received_above_poverty_level','not_received_total',
                   'not_received_below','not_received_above'],
                  ['location','geo_id','total','total_native','total_foreign_born','foreign_born_us_ctzn','foreign_born_not_us_ctzn'],
                  ['location','geo_id','total_household','family_household','non_family_household'],
                  ['location','geo_id','total_household','family_household','non_family_household'],
                  ['location','geo_id','total','total_had_birth_last_12m','total_did_not','total_white','white_had_birth','white_did_not',
                   'total_black','black_had_birth','black_did_not','total_indian_alaskan','ind_ala_had_birth','ind_ala_did_not',
                   'total_asian','asian_had_birth','asian_did_not','total_pacific','pacific_had_birth','pacific_did_not',
                   'total_other','other_had_birth','other_did_not','total_two_race','two_race_had_birth','two_race_did_not',
                   'total_white_no_latin','white_not_latin_had_birth','white_not_latin_did_not','total_latin',
                   'latin_had_birth','latin_did_not'],
                  ['location','geo_id','total_poverty','tot_income_below_p_level','male_income_below_p_level','male_lt_high_shcool',
                   'male_high_school','male_college','male_ge_bachelore','female_income_below_p_level','female_lt_high_shcool',
                   'female_high_school','female_college','female_ge_bachelore'],
                   ['location','geo_id','total_population','total_male','male_no_school','male_4th_grade','male_5th6th_grade','male_7th8th_grade',
                    'male_9th_grade','male_10th_grade','male_11th_grade','male_12th_grade','male_high_school',
                    'male_lt_1year_college','male_some_college_no_degree','male_associate_degree','male_bachlore_degree',
                    'male_master+degree','male_professional_school_degree','male_doctorate_degree',
                    'total_female', 'female_no_school', 'female_4th_grade', 'female_5th6th_grade', 'female_7th8th_grade',
                    'female_9th_grade', 'female_10th_grade', 'female_11th_grade', 'female_12th_grade', 'female_high_school',
                    'female_lt_1year_college', 'female_some_college_no_degree', 'female_associate_degree',
                    'female_bachlore_degree','female_master+degree', 'female_professional_school_degree',
                    'female_doctorate_degree'],
                  ['location','geo_id','total_built','built_2014_or_later','built_2010_to_2013','built_2000_2009','built_1990_1999','built_1980_1989',
                   'built_1970_1979','built_1960_1969','built_1950_1959','built_1940_1949','built_1939_or_earlier']]


output_table_names = ['median_earning_by_education','percentage_of_veterans_by_state','household_receiving_food_stamps_by_state',
                      'native_vs_foreign_born_by_county','total_number_of_households_by_county',
                      'total_number_of_households_by_zip','fertility_by_race','poverty_rate_by_education_by_state',
                      'education_by_gender','year_structure_built']

req_year = [2017,2012]
req_file = 'acs5'
tables_for_other_years = [0,1,2,4,5,6,7,8]
f_indx = 0
y_indx = 0
while f_indx in range(len(output_table_names)):

    dfdata = censusdata.download(req_file, req_year[y_indx], censusdata.censusgeo([(geo_attr[f_indx], '*')]),requested_columns[f_indx])

    dfdata.reset_index (inplace=True)  # this line adds new index and makes old index part of the table
    dfdata.columns = output_columns[f_indx]
    dfdata['survey_file']= req_file
    dfdata['survey_year'] = req_year[y_indx]
#    print (output_table_names[f_indx])
#    print ("table rows:columns",dfdata.shape)
    if geo_attr[f_indx] == "state": # added if statement to seperate location column to different columns per geoptype
        dfdata[['state', 'Sum_lvl',"lvl","stateID"]] = dfdata.location.apply (lambda x: pd.Series (str (x).split (":")))
    if geo_attr[f_indx] == "county":
        dfdata[['county', 'Sum_lvl',"lvl","stateID","county_id"]] = dfdata.location.apply (lambda x: pd.Series (str (x).split (":")))
    if geo_attr[f_indx] == 'zip+code+tabulation+area':
        dfdata[['zcta', 'Sum_lvl',"lvl","zcta_ID"]] = dfdata.location.apply (lambda x: pd.Series (str (x).split (":")))
#    print (output_table_names[f_indx])
    dfdata.drop ('location', axis=1, inplace=True) #drop the location because have problems sending to SQL
#    dfdata.to_csv ("C:\\Temp\\output\\df_ouput_19.csv", encoding='utf-8')
    dfdata.to_sql(output_table_names[f_indx], engine, if_exists='replace', index=False) #
#    print(dfdata.shape)
    if f_indx in tables_for_other_years and y_indx == 0:
        y_indx += 1
        hold_dfdata = dfdata
#        print(hold_dfdata.shape)
        continue
    if y_indx > 0:
        y_indx = 0
        dfdata = hold_dfdata.append(dfdata)
#    print(dfdata.shape)
    dfdata.to_sql(output_table_names[f_indx], engine, if_exists='replace', index=False)
    f_indx += 1
    print ("finished round ", f_indx )
print ("finished! competely!!")

sql = """
select count(*) from median_earning_by_education
"""
df = pd.read_sql(sql, con=engine)
print(df)