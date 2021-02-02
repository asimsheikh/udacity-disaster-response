# import libraries

import sys

import pandas as pd
from sqlalchemy import create_engine


def load_data(messages_filepath: str, categories_filepath: str) -> pd.DataFrame:
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)

    df = pd.merge(messages, categories, on="id")

    categories = categories["categories"].str.split(";", expand=True)

    category_colnames = categories.iloc[0].apply(lambda text: text[:-2])
    categories.columns = category_colnames

    for column in categories:
        categories[column] = categories[column].apply(lambda text: text.split("-")[-1])
        categories[column] = categories[column].astype(int)

    df.drop("categories", axis="columns", inplace=True)

    df = pd.concat([df, categories], axis="columns")

    filt = df.duplicated()
    df = df[~filt]
    assert (
        df.duplicated().sum() == 0
    ), "There are still duplicate rows in your DataFrame"
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    return df


def save_data(df: pd.DataFrame, database_filename: str):
    engine = create_engine("sqlite:///{}".format(database_filename))
    df.to_sql("messages", engine, index=False)


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print(
            "Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}".format(
                messages_filepath, categories_filepath
            )
        )
        df = load_data(messages_filepath, categories_filepath)

        print("Cleaning data...")
        df = clean_data(df)

        print("Saving data...\n    DATABASE: {}".format(database_filepath))
        save_data(df, database_filepath)

        print("Cleaned data saved to database!")

    else:
        print(
            "Please provide the filepaths of the messages and categories "
            "datasets as the first and second argument respectively, as "
            "well as the filepath of the database to save the cleaned data "
            "to as the third argument. \n\nExample: python process_data.py "
            "disaster_messages.csv disaster_categories.csv "
            "DisasterResponse.db"
        )


if __name__ == "__main__":
    main()
