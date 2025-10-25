import pandas as pd 
import os
import json

cur_dir = os.path.dirname(os.path.abspath(__file__))

data_df = pd.read_csv(rf'{cur_dir}\data.csv')
columns = data_df.columns.tolist()

data_df = pd.melt(data_df,
                  id_vars = columns[:5],
                  var_name = 'TIME_PERIOD', 
                  value_name = 'OBS_VALUE')

print(f'\n1. Додано 2 нових колонки: TIME_PERIOD та OBS_VALUE\n\n{data_df.head()}\n')
                
count_2021 = data_df.query("TIME_PERIOD == '2021'")['OBS_VALUE'].notna().sum()
count_2020 = data_df.query("TIME_PERIOD == '2020'")['OBS_VALUE'].notna().sum()
 
cng = round((count_2021 / count_2020) * 100, 2)

if cng > 100:
    print(f"2. Рівень заповненості звітності в 2021 році в порівнянні з 2020 роком виріс на {cng}%")
elif cng == 100:
    print(f"2. Рівень заповненості звітності в 2021 році в порівнянні з 2020 роком не змінився")
elif cng < 100:
    print(f"2. Рівень заповненості звітності в 2021 році в порівнянні з 2020 роком зменшився, і становить {cng}%")

edrpou_df = pd.read_csv(rf'{cur_dir}\edrpou.csv')

merged_df = data_df.merge(edrpou_df, how='left', left_on='EDRPOU.id', right_on='EDRPOU').drop(columns='EDRPOU')

print(f'\n3. Додано атрибути КВЕД та КАТОТТГ\n\n{merged_df.head()}\n')

merged_df['OBL.id'] = merged_df['KATOTTG'].str[2:4]

grouped_df = merged_df.groupby(['HAZARD.id', 'MATERIAL.id', 'WASTE.id', 'TIME_PERIOD', 'OBL.id'])['OBS_VALUE'].sum().reset_index(name = 'OBS_VALUE')

print(f"\n4. Розраховано об'єм утворених відходів для кожного класу небезпеки, матеріалу відходу, виду відходу, року звітності на рівні області \n\n{grouped_df.head()}\n")

grouped_df = merged_df.groupby(['HAZARD.id', 'KVED', 'TIME_PERIOD', 'OBL.id'])['OBS_VALUE'].sum().reset_index(name = 'OBS_VALUE')

print(f"\n5. Розраховано об'єм утворених відходів для кожного класу небезпеки, виду економічної діяльності, року звітності на рівні області \n\n{grouped_df.head()}\n")

grouped_df = merged_df.groupby(['TIME_PERIOD', 'OBL.id'])['OBS_VALUE'].sum().reset_index(name = 'OBS_VALUE')

grouped_df['OBS_VALUE'] = grouped_df['OBS_VALUE'].map(lambda x: f'{x:,.2f}')

print(f"\n6. Розраховано об'єм утворених відходів для кожного року звітності на рівні області \n\n{grouped_df.head()}\n")

grouped_df = merged_df.groupby('OBL.id')['OBS_VALUE'].sum().reset_index(name = 'OBS_VALUE')

grouped_df['Percentage'] = (grouped_df['OBS_VALUE'] / grouped_df['OBS_VALUE'].sum()) * 100

print(f'В регіоні {grouped_df.loc[grouped_df['Percentage'].idxmax(), 'OBL.id']} утворилаcь найбільша кількість відходів: {round(grouped_df['Percentage'].max(), 2)} %')

class_df = pd.read_json(rf'{cur_dir}\SSSU_CL_WASTE_HAZARD_CLASS(2.0.1).json')
class_df = pd.json_normalize(class_df['codes'], sep='_')

merged_df['HAZARD.id'] = merged_df['HAZARD.id'].astype(str).str.zfill(4)

merged_df = merged_df.merge(class_df, how = 'left', left_on = 'HAZARD.id', right_on = 'id').drop(columns=['id', 'annotations', 'names_en'])

print(f"\n7. Додано атрибути name, parent  \n\n{merged_df.head()}\n")

grouped_df = merged_df.groupby(['TIME_PERIOD', 'parent'])['OBS_VALUE'].sum().reset_index(name = 'OBS_VALUE')

total_df = grouped_df.merge(class_df, how = 'left', left_on = 'parent', right_on = 'id')
total_df = total_df.groupby(['TIME_PERIOD', 'parent_y'])['OBS_VALUE'].sum().reset_index(name = 'OBS_VALUE')
total_df = total_df.rename(columns={'parent_y': 'parent'})
result_df = pd.concat([grouped_df, total_df], ignore_index=True)

result_df['OBS_VALUE'] = result_df['OBS_VALUE'].map(lambda x: f'{x:,.2f}')

result_df = result_df.merge(class_df[['id', 'name']], how='left', left_on='parent', right_on='id')

print(f"\n8. Обраховано об'єм утворених відходів для груп класів небезпеки \n\n {result_df[['TIME_PERIOD', 'name', 'OBS_VALUE']]}")


