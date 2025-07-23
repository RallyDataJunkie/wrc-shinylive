import asyncio
import sqlite3
from typing import Any, Awaitable

from pandas import read_sql_query, DataFrame

from shiny import reactive
from shiny.express import render, ui

import requests


def get_data() -> list:
    return requests.get(
        "https://webappsdata.wrc.com/srv/wrc/json/api/liveservice/getData?timeout=5000"
    ).json()["_entries"]


# === Initialize the database =========================================

ui.help_text(str(get_data()[0]))


def init_db(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    try:
        cur.executescript(
            """
            CREATE TABLE wrctimes (name text, lon real, lat real, speed real, heading real, utx int, driverid text, track text, status text, gear int, throttle int, brk real, rpm int, accx real, accy real, kms real, altitude int);
            CREATE INDEX idx_wrcdata ON wrctimes (driverid, utx);
            """
        )

    #     items = get_data()
    #     data = [
    #         (
    #             record["name"],
    #             record["lon"],
    #             record["lat"],
    #             record["speed"],
    #             record["heading"],
    #             record["utx"],
    #             record["driverid"],
    #             record["track"],
    #             record["status"],
    #             record["gear"],
    #             record["throttle"],
    #             record["brk"],
    #             record["rpm"],
    #             record["accx"],
    #             record["accy"],
    #             record["kms"],
    #             record["altitude"],
    #         )
    #         for record in items
    #     ]
    #     cur.executemany(
    #         """
    # INSERT INTO wrctimes
    # (name, lon, lat, speed, heading, utx, driverid, 
    #     track, status, gear, throttle, brk, rpm, 
    #     accx, accy, kms, altitude) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    # """,
    #         data,
    #     )
        con.commit()
        print("db initialised")
    finally:
        cur.close()


conn = sqlite3.connect(":memory:")
init_db(conn)


# === Randomly update the database with an asyncio.task ==============


def update_db(con: sqlite3.Connection) -> None:
    """Update db with grabbed data."""

    cur = con.cursor()
    try:
        items = get_data()
        data = [
            (
                record["lon"],
                record["lat"],
                record["speed"],
                record["heading"],
                record["utx"],
                record["driverid"],
                record["track"],
                record["status"],
                record["gear"],
                record["throttle"],
                record["brk"],
                record["rpm"],
                record["accx"],
                record["accy"],
                record["kms"],
                record["altitude"],
                record["name"],
            )
            for record in items
        ]
        cur.executemany(
            """
    UPDATE wrctimes
    SET lon = ?, lat = ?, speed = ?, heading = ?, utx = ?, driverid = ?, 
        track = ?, status = ?, gear = ?, throttle = ?, brk = ?, rpm = ?, 
        accx = ?, accy = ?, kms = ?, altitude = ?
    WHERE name = ?
    """,
            data,
        )
        con.commit()
    finally:
        cur.close()


import time


async def update_db_task(con: sqlite3.Connection) -> Awaitable[None]:
    """Task that alternates between sleeping and updating telemetry."""
    while True:
        await asyncio.sleep(5)
        update_db(con)


_ = asyncio.create_task(update_db_task(conn))


# === Create the reactive.poll object ===============================


def tbl_last_modified() -> Any:
    df = read_sql_query("SELECT MAX(utx) AS utx FROM wrctimes", conn)
    return df["utx"].to_list()


@reactive.poll(tbl_last_modified, 5.1)
def car_getdata() -> DataFrame:
    return read_sql_query("SELECT * FROM wrctimes", conn)


with ui.card():
    ui.markdown(
        """
        # `shiny.reactive.poll` demo

        This example app shows how to stream results from a database (in this
        case, an in-memory sqlite3) with the help of `shiny.reactive.poll`.
        """
    )

    @render.data_frame
    def table():
        df = car_getdata()
        return df
