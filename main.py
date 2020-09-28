import sys, os
import pandas as pd
import numpy as np
from datetime import datetime
import dateutil.relativedelta
import xlrd
from xlrd import open_workbook
from pandas import ExcelWriter
from openpyxl import load_workbook

if len(sys.argv) != 2:
    print("Incorrect arguments. Please input file name")
    sys.exit(0)

excel_file_path = sys.argv[1]
if not (os.path.exists(excel_file_path) and os.path.isfile(excel_file_path)):
    print("Incorrect file path")
    sys.exit(0)

sheet_name = "Extract"
converters = {'IP Name':str, 'IPN#':str, 'Role':str, 'P-Society':str, 'P-Share':str, 'M-Society':str, 'M-Share': str}

writer = ExcelWriter(excel_file_path, engine='openpyxl')
book = load_workbook(excel_file_path)
wb = open_workbook(excel_file_path)
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
Extract_sheet = wb.sheet_by_name(sheet_name)

# create empty dataframe
total_df = pd.DataFrame(columns=['Title', 'ISWC', 'SOCIETY', 'IP Name', 'IPN#', 'Role', 'P-Society', 'P-Share', 'M-Society', 'M-Share'])
# total_df = pd.DataFrame()
total_creator_df = pd.DataFrame(columns=['IP Name', 'IPN#', 'Role', 'P-Society', 'P-Share', 'M-Society', 'M-Share'])

# 1. Get New Dataset

row = 0
title_cnt = 0
while row  < Extract_sheet.nrows:
    # get first cell of each row
    first_cell_of_row = Extract_sheet.cell_value(row, 0)
    # if first cell of row is not empty, it means new Title starts from its row
    title = ''
    if first_cell_of_row:
        title_cnt += 1
        for col in range(0, Extract_sheet.ncols):
            cell_value = str(Extract_sheet.cell_value(row, col))
            if 'Title' in cell_value:
                title = Extract_sheet.cell_value(row, col+1)
    else: 
        row += 1
        continue

    # get ISWC, Society, Last Update
    iswc = ''
    society = ''
    last_update = ''
    for row1 in range(row + 1, Extract_sheet.nrows):
        valid_row = False
        for col in range(0, Extract_sheet.ncols):
            cell_value = str(Extract_sheet.cell_value(row1, col))
            if cell_value.strip():
                valid_row = True
            if 'ISWC' in cell_value:
                iswc = Extract_sheet.cell_value(row1, col + 1)
            if 'Submitting Society' in cell_value:
                society = str(Extract_sheet.cell_value(row1, col + 1))
            if 'Society Work Code' in cell_value:
                society_code = str(Extract_sheet.cell_value(row1, col + 1))
            if 'Last Update' in cell_value:
                last_update = Extract_sheet.cell_value(row1, col + 1)
        # check if row is empty
        if not valid_row:
            row1 += 1
            break
    # find Creater dataframe
    creater_st_row = row1 + 1
    creater_en_row = row1 + 1
    for row1 in range(creater_st_row, Extract_sheet.nrows):
        next_paragraph_cell = Extract_sheet.cell_value(row1, 2)
        if 'Creator(s)' in next_paragraph_cell:
            creater_st_row = row1 + 1
        if 'Publisher(s)' in next_paragraph_cell:
            creater_en_row = row1 - 2
            break
    creator_df = pd.DataFrame(columns=['IP Name', 'IPN#', 'Role', 'P-Society', 'P-Share', 'M-Society', 'M-Share'])
    for row1 in range(creater_st_row + 1, creater_en_row+1):
        row_list = Extract_sheet.row_values(row1, start_colx=2, end_colx=None)
        creator_df.loc[len(creator_df)] = row_list

    # creator_df = pd.read_excel(excel_file_path, index_col = None, skiprows= creater_st_row, 
    #     nrows= creater_en_row - creater_st_row, sheet_name=sheet_name, usecols=range(2,Extract_sheet.ncols),
    #     converters=converters)

    # assign Title, ISWC, SOCIETY in creator dataframe
    creator_df = creator_df.assign(SOCIETY=society)[['SOCIETY'] + creator_df.columns.tolist()]
    creator_df = creator_df.assign(ISWC=iswc)[['ISWC'] + creator_df.columns.tolist()]
    creator_df = creator_df.assign(Title=title)[['Title'] + creator_df.columns.tolist()]
    creator_df = creator_df.assign(LAST_UPDATE=last_update)

    # merge Dataframes
    total_df = pd.concat([total_df, creator_df])
    # merge creator dataframe
    creator_df = creator_df.assign(Title_No=title_cnt)[['Title_No'] + creator_df.columns.tolist()]
    creator_df = creator_df.assign(Society_Code=society_code)[['Society_Code'] + creator_df.columns.tolist()]
    total_creator_df = pd.concat([total_creator_df, creator_df])


    # find Publisher dataframe
    publisher_st_row = row1 + 1
    publisher_en_row = row1 + 1
    for row1 in range(publisher_st_row, Extract_sheet.nrows):
        next_paragraph_cell = Extract_sheet.cell_value(row1, 2)
        if 'Publisher(s)' in next_paragraph_cell:
            publisher_st_row = row1 + 1
        if 'Performer(s)' in next_paragraph_cell:
            publisher_en_row = row1 - 2
            break
    publisher_df = pd.DataFrame(columns=['IP Name', 'IPN#', 'Role', 'P-Society', 'P-Share', 'M-Society', 'M-Share'])
    for row1 in range(publisher_st_row + 1, publisher_en_row+1):
        row_list = Extract_sheet.row_values(row1, start_colx=2, end_colx=None)
        publisher_df.loc[len(publisher_df)] = row_list

    # publisher_df = pd.read_excel(excel_file_path, index_col = None, skiprows= publisher_st_row, 
    #     nrows= publisher_en_row - publisher_st_row, sheet_name=sheet_name, usecols=range(2,Extract_sheet.ncols), 
    #     converters=converters)
    row = row1
    # assign Title, ISWC, SOCIETY in publisher dataframe
    publisher_df = publisher_df.assign(SOCIETY=society)[['SOCIETY'] + publisher_df.columns.tolist()]
    publisher_df = publisher_df.assign(ISWC=iswc)[['ISWC'] + publisher_df.columns.tolist()]
    publisher_df = publisher_df.assign(Title=title)[['Title'] + publisher_df.columns.tolist()]
    publisher_df = publisher_df.assign(LAST_UPDATE =last_update)

    # merge Dataframes
    total_df = pd.concat([total_df, publisher_df])

total_df.to_excel(writer,'New Dataset Python', index=False)

# 2. Stage One

creator_group_df = total_creator_df.groupby('IP Name')['IPN#'].apply(set).reset_index(name='IPN list')
stage_one_df = pd.DataFrame(columns=['IP Name', 'IPN#'])
for i, row in creator_group_df.iterrows():
    IPN_list = list(row['IPN list'])
    if len(IPN_list) > 1 or (len(IPN_list) == 1 and np.nan in IPN_list):
        for entry in IPN_list:
            stage_one_df = stage_one_df.append({'IP Name':row['IP Name'], 'IPN#':entry}, ignore_index=True)
stage_one_df = stage_one_df.replace(np.nan, 'Blank', regex=True)
stage_one_df.to_excel(writer, 'Stage One', index=False)


# 3. Stage Two
title_group_df = total_creator_df.groupby(['Title_No', 'Title', 'ISWC']).size().reset_index(name='counts')
title_group_df = title_group_df.groupby(['Title', 'ISWC'])['counts'].apply(set).reset_index(name='count_list')
# title_group_df['count_list'] = title_group_df['count_list'].astype(str)
stage_two_df = pd.DataFrame(columns=['Title', 'No. of Creators', 'ISWC'])
for i, row in title_group_df.iterrows():
    creator_count_list = list(row['count_list'])
    if len(creator_count_list) > 1:
        for entry in creator_count_list:
            stage_two_df = stage_two_df.append({'Title':row['Title'], 'No. of Creators':entry, 'ISWC':row['ISWC']}, ignore_index=True)
stage_two_df.to_excel(writer, 'Stage Two', index=False)

# 4. Stage Three
stage_three_df = total_creator_df.loc[(total_creator_df['ISWC'] == 'No preferred') | (total_creator_df['ISWC'] == '')]
stage_three_df = stage_three_df.groupby(['Title', 'Society_Code']).size().reset_index(name='counts').drop(['counts'], axis=1)
stage_three_df.to_excel(writer, 'Stage Three', index=False)

# 5. Stage Four
end_time = datetime.now()
end_date = end_time.strftime('%Y/%m/%d')
start_time = end_time - dateutil.relativedelta.relativedelta(months=2)
start_date = start_time.strftime('%Y/%m/%d')
mask = ((total_creator_df['LAST_UPDATE'] > start_date) & (total_creator_df['LAST_UPDATE'] <= end_date)) |(total_creator_df['LAST_UPDATE'] == '')
stage_four_df = total_creator_df.loc[mask].groupby(['Title', 'LAST_UPDATE', 'ISWC']).size().reset_index(name='counts').drop(['counts'], axis=1)
stage_four_df.to_excel(writer, 'Stage Four', index=False)

# 6. Stage Five
# group by title and creator
shared_df = total_creator_df.loc[total_creator_df['P-Share'] != '*']
shared_df = shared_df.copy()
shared_df['P-Share'] = shared_df['P-Share'].astype('float')
shared_df = shared_df.groupby(['Title_No', 'Title', 'IP Name'])['P-Share'].sum().reset_index(name='%')#.apply(list)
stage_five_df = shared_df.drop_duplicates(subset=['Title', 'IP Name', '%'], keep=False).drop(['Title_No'], axis=1)
stage_five_df = stage_five_df.rename(columns={'IP Name':'Creator'})
stage_five_df.to_excel(writer, 'Stage Five', index=False)

# 7. Save all sheets
writer.save()