from __future__ import annotations

import numpy as np
import pandas as pd
from keras.layers import Dense
from keras.models import Sequential
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score


def create_model(optimizer: str = "adam", loss: str = "mean_squared_error") -> Sequential:
    model = Sequential()
    model.add(Dense(27, activation="relu"))
    model.add(Dense(13, activation="relu"))
    model.add(Dense(6, activation="relu"))
    model.add(Dense(1))
    model.compile(optimizer=optimizer, loss=loss)
    return model


def GetMetrics(y: list[float], predictions: list[float]) -> dict[str, float]:
    return {
        "MSE": mean_squared_error(y, predictions),
        "RMSE": np.sqrt(mean_squared_error(y, predictions)),
        "MAE": mean_absolute_error(y, predictions),
        "R2": r2_score(y, predictions),
    }


def haversine_vector(row: pd.core.series.Series) -> float:
    lat1, lon1 = row["Restaurant_latitude"], row["Restaurant_longitude"]
    lat2, lon2 = row["Delivery_location_latitude"], row["Delivery_location_longitude"]
    R = 6371  # raio da Terra

    if None in (lat1, lon1, lat2, lon2) or np.nan in (lat1, lon1, lat2, lon2):
        return np.nan

    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)

    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    return R * c
