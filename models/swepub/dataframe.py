from typing import Optional

import pandas as pd  # type: ignore
from pandas import DataFrame  # type: ignore
from pydantic import BaseModel


class SwepubDataframe(BaseModel):
    dataframe: Optional[DataFrame]
    pickle_filename: str = "swepub.pkl.gz"

    class Config:
        arbitrary_types_allowed = True

    def load_into_memory(self):
        self.dataframe = pd.read_pickle(self.pickle_filename)

    def export_subjects_dataframe(self, pickle_filename: str = "subjects.pkl.gz"):
        """Takes a pickle filename and export all subjects as a dataframe to it"""
        if self.dataframe is None:
            raise ValueError("self.dataframe was None")
        df_subjects = pd.DataFrame()
        for list in self.dataframe["subjects"]:
            for item in list:
                df_subjects = df_subjects.append(
                    item.export_dataframe()
                )
        df_subjects.to_pickle(pickle_filename)
        print(df_subjects.describe())
        print(df_subjects.sample(3))