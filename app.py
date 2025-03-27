from __future__ import annotations

import pandas as pd

x = pd.DataFrame({"x": [1, 2, 3], "y": [3, 4, 5]})
print(type(x.iloc[1]))
