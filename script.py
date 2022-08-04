import pandas as pd
import numpy as np
 
data = pd.read_csv('/home/ndx/Innowise tasks/Innowise_task_5/data/tiktok_google_play_reviews_output.csv')

data.to_dict('records')
