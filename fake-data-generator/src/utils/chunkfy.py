"""
Module to Split DataFrame into Chunks
"""
import pandas as pd
 
class Chunkfy:
    def __init__(self, df:pd.DataFrame, chunksize:int) -> pd.DataFrame:
        """
        init function
        parameters
        ----------
        df: pandas dataframe
        chunksize: size of each chunk
        """
        self.df = df
        self.chunksize = chunksize
        self.start = 0
        self.length = df.shape[0]
        self.gen_df = None
        
        def gen_data(self):
            if self.start >= self.length:
                raise StopIteration
            else:
                if self.length <= self.chunksize:
                    self.gen_df = self.df
                    self.start = self.length
                if self.start + self.chunksize <= self.length:
                    self.gen_df = self.df[self.start:self.start+self.chunksize]
                    self.start = self.chunksize
                if self.start < self.length:
                    self.gen_df = self.df[self.start:]
                    self.start = self.length
                return self.gen_df