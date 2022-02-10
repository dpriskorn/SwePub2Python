# Extract subjects from the pickle
import pandas as pd

# TODO load the df first

df_subjects = pd.DataFrame()
for list in df["subjects"]:
    for item in list:
        df_subjects = df_subjects.append(
            item.export_dataframe()
        )
df_subjects.to_pickle(f"subjects-from-swepub.pkl.gz")
print(df_subjects.describe())
print(df_subjects.sample(3))
