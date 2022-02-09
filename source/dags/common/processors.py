import os

import pandas as pd


def process_gdp_data(input_file_path: str, destination_folder_path: str, output_file_name: str):
    df = pd.read_csv(input_file_path)

    df = df.drop(columns=['2019'])
    df = df.rename(columns={'Country ': 'Country'})

    available_years = df.columns[2:]  # 31 years
    available_years_df = pd.DataFrame(available_years, columns=['Year'])

    df_computed = df.merge(available_years_df, how='cross')
    df_computed = df_computed.drop(columns=df.columns[1:])

    concatenated_years = df.iloc[0][available_years]
    for i in range(1, df.shape[0]):
        concatenated_years = pd.concat((concatenated_years, df.iloc[i][available_years]), axis=0)

    concatenated_years = concatenated_years.reset_index()
    result = pd.concat((df_computed, concatenated_years), axis=1, ignore_index=True)

    result.columns = ['Country', 'Year', 'Year2', 'GDP']
    result = result.drop(columns=['Year2'])

    print(f'Result of processing {input_file_path} is:')
    print(f'\n{result.head()}')

    destination_path = os.path.join(destination_folder_path, output_file_name)
    result.to_csv(destination_path, index_label='id', index=True)


def process_internet_data(input_file_path: str, destination_folder_path: str, output_file_name: str):
    df = pd.read_csv(input_file_path)

    df = df.rename(columns={
        'Number of internet users (OWID based on WB & UN)': 'Number_of_internet_users',
        'Entity': 'Country'
    })

    print(f'Result of processing {input_file_path} is:')
    print(f'\n{df.head()}')

    destination_path = os.path.join(destination_folder_path, output_file_name)
    df.to_csv(destination_path, index_label='id', index=True)


def process_suicide_data(input_file_path: str, destination_folder_path: str, output_file_name: str):
    df = pd.read_csv(input_file_path)

    df = df.drop(columns=['country-year', ' gdp_for_year ($) ', 'gdp_per_capita ($)', 'HDI for year', 'suicides/100k pop'])
    df = df.rename(columns={
        'suicides_no': 'suicides_number',
        'country': 'Country'
    })

    for column in df.columns:
        df = df.rename(columns={column: column.capitalize()})

    print(f'Result of processing {input_file_path} is:')
    print(f'\n{df.head()}')

    destination_path = os.path.join(destination_folder_path, output_file_name)
    df.to_csv(destination_path, index_label='id', index=True)


def process_data(gdp_file_path: str, suicide_file_path: str, internet_file_path: str, output_folder_path: str):
    suicide = pd.read_csv(suicide_file_path)
    gdp = pd.read_csv(gdp_file_path)
    internet = pd.read_csv(internet_file_path)

    suicide = suicide.drop(columns=['id'])
    gdp = gdp.drop(columns=['id'])
    internet = internet.drop(columns=['id'])

    result = pd.merge(gdp, suicide, how='outer', on=['Country', 'Year'])

    aggregated_populations = result.groupby(['Country', 'Year']) \
        .sum('Population') \
        .reset_index()[['Country', 'Year', 'Population']]

    countries = result['Country'].unique()

    for country in countries:
        aggregated_populations.loc[
            (aggregated_populations['Population'] == 0)
            & (aggregated_populations['Country'] == country),
            'Population'
        ] = aggregated_populations[
            ~(aggregated_populations['Population'] == 0)
            & (aggregated_populations['Country'] == country)
            ].mean()['Population']

    result = pd.merge(result, aggregated_populations, how='inner', on=['Country', 'Year'])

    result = result.rename(columns={
        'Population_x': 'Number_of_people',
        'Population_y': 'Total_population'
    })

    result['Percentage_of_population'] = result['Number_of_people'] / result['Total_population']

    result = pd.merge(result, internet, how='left', on=['Country', 'Year'])

    result = result.rename(columns={
        'Number_of_internet_users': 'Total_number_of_internet_users'
    })

    result['Number_of_internet_users'] = result['Percentage_of_population'] * result['Total_number_of_internet_users']

    generations_table = pd.DataFrame(result['Generation'].unique(), columns=['Generation']).dropna()
    sex_table = pd.DataFrame(result['Sex'].unique(), columns=['Sex']).dropna()
    age_table = pd.DataFrame(result['Age'].unique(), columns=['Age_category']).dropna()
    year_table = pd.DataFrame(result['Year'].sort_values().unique(), columns=['Year']).dropna()
    country_table = result[['Country', 'Code']].drop_duplicates(subset=['Country'])

    year_table['Century'] = year_table['Year'] // 100
    year_table['Decade'] = (year_table['Year'] // 10 * 10).astype(str)
    year_table['Decade'] = year_table['Decade'].astype(str) + 's'

    result['Generation'] = result['Generation'].apply(
        lambda gen: gen if pd.isna(gen) else generations_table.index[generations_table['Generation'] == gen][0]
    )

    result['Year'] = result['Year'].apply(
        lambda year: year_table.index[year_table['Year'] == year][0]
    )

    result['Sex'] = result['Sex'].apply(
        lambda sex: sex if pd.isna(sex) else sex_table.index[sex_table['Sex'] == sex][0]
    )

    result['Age'] = result['Age'].apply(
        lambda age: age if pd.isna(age) else age_table.index[age_table['Age_category'] == age][0]
    )

    result['Country'] = result['Country'].apply(
        lambda c: c if pd.isna(c) else country_table.index[country_table['Country'] == c][0]
    )

    result = result.rename(columns={'Age': 'Age_category'})
    result = result.drop(columns=['Code'])

    print(f'Saving files to the {output_folder_path}')

    country_depending_facts = result[['Country', 'Year', 'GDP', 'Total_population', 'Total_number_of_internet_users']]
    people_depending_facts = result[
        ['Country', 'Year', 'Sex', 'Age_category', 'Suicides_number', 'Number_of_people',
         'Generation', 'Percentage_of_population', 'Number_of_internet_users']
    ]

    generations_table.to_csv(os.path.join(output_folder_path, 'generations.csv'), index_label='id', index=True)
    year_table.to_csv(os.path.join(output_folder_path, 'years.csv'), index_label='id', index=True)
    sex_table.to_csv(os.path.join(output_folder_path, 'sexes.csv'), index_label='id', index=True)
    country_table.to_csv(os.path.join(output_folder_path, 'countries.csv'), index_label='id', index=True)
    age_table.to_csv(os.path.join(output_folder_path, 'age_categories.csv'), index_label='id', index=True)
    people_depending_facts.to_csv(os.path.join(output_folder_path, 'people_depending_facts.csv'), index_label='id', index=True)
    country_depending_facts.to_csv(os.path.join(output_folder_path, 'country_depending_facts.csv'), index_label='id', index=True)
