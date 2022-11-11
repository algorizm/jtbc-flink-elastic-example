from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.expressions import col

import pandas as pd
import numpy as np

config = Configuration()
config.set_string('execution.buffer-timeout', '1 min')
env_settings = EnvironmentSettings \
	.new_instance() \
	.in_batch_mode().build()

table_env = TableEnvironment.create(env_settings)

# Create a PyFlink Table
pdf = pd.DataFrame(np.random.rand(1000, 2))
table = table_env.from_pandas(pdf,
							  DataTypes.ROW([DataTypes.FIELD("a", DataTypes.DOUBLE()),
                           DataTypes.FIELD("b", DataTypes.DOUBLE())])) \
							.filter(col('a') > 0.5)

# Convert the PyFlink Table to a Pandas DataFrame
pdf = table.limit(100).to_pandas()
print(pdf.head(100))

pdf2 = pdf.query("a > 0.9").copy().sort_index(axis=1)
print(pdf2.head())

