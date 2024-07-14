import sqlite3
from atexit import register, unregister
from os import path, popen
from loguru import logger
from json import dumps
from dataclasses import dataclass

__name__ = "Database"
sqlite3.threadsafety = 3
db = sqlite3.connect("database.db", check_same_thread=False)

sqlite3.enable_callback_tracebacks(True)

cursor = db.cursor() # Get cursor

cursor.execute('''
CREATE TABLE IF NOT EXISTS Music (
name TEXT PRIMARY KEY,
title TEXT NOT NULL,
artist TEXT NOT NULL,
id INTEGER
)
''') # Create music table

cursor.execute('''
CREATE TABLE IF NOT EXISTS Music_info (
id INTEGER PRIMARY KEY,
file_mp3 TEXT NOT NULL,
file_flac TEXT NOT NULL,
json_meta TEXT NOT NULL
)
''') # Create music_info table

cursor.execute('''
CREATE TABLE IF NOT EXISTS config (
name TEXT PRIMARY KEY,
value TEXT NOT NULL
)
''') # Create config table

cursor.execute('CREATE INDEX IF NOT EXISTS idx_name ON Music (name)') # Create index for optimizing search
cursor.execute('CREATE INDEX IF NOT EXISTS idx_id ON Music (id)') # Create index for optimizing search x2
cursor.execute('CREATE INDEX IF NOT EXISTS idx_id2 ON Music_info (id)') # Create index for optimizing search x3

db.commit() # Commit all changes so it not be lost

DB_SIZE = """
SELECT 
    SUM(total_bytes) AS "Total Size (Bytes)"
FROM (
    SELECT 
        name,
        (page_count * page_size) AS total_bytes
    FROM 
        pragma_page_size
    JOIN 
        pragma_page_count ON 1 = 1
    JOIN 
        sqlite_master ON type = 'table'
    WHERE 
        name NOT LIKE 'sqlite_%'
);
"""

cursor.execute(DB_SIZE)

logger.success(f"Loaded database, size(in bytes): {cursor.fetchone()[0]}")

FFMPEG_CONV_TO_MP3 = "ffmpeg -i {inp} -f mp3 {out}"
FFMPEG_CONV_TO_FLAC = "ffmpeg -i {inp} -f flac {out}"

INSERT_MUSIC = "INSERT INTO Music (name, title, artist, id) VALUES (?, ?, ?, ?)"
INSERT_MUSIC_INFO = "INSERT INTO Music_info (id, file_mp3, file_flac, json_meta) VALUES (?, ?, ?, ?)"

GET_BY_ID = "SELECT * FROM Music WHERE ? = id"
GET_BY_ID2 = "SELECT * FROM Music_info WHERE ? = id"

SEARCH_BY_NAME = "SELECT id, instr(name, ?) position FROM Music WHERE position > 0"

@dataclass
class MusicInfo:
    id: int
    name: str
    title: str
    artist: str
    file_mp3: str
    file_flac: str

def close(): # Closes database
    logger.info("Closing database...")
    db.close() # Close database(NO RECURSION)
    unregister(_on_exit) # Unregister _on_exit function(it becomes useless)
    logger.success("Database closed")

def get_config(name: str, default: str=None, create: bool=False) -> str: # Get config
    cursor.execute("SELECT * FROM config WHERE ? = name", (name,)) # Get config value
    data = cursor.fetchone()
    if data == None: # If no config value exists...
        if default == None: # And default not set...
            raise KeyError(name) # Raise KeyError exception
        else: # If default set
            if create: # If create set to True...
                cursor.execute("INSERT INTO config (name, value) VALUES (?, ?)", (name, default)) # Set config value
                db.commit() # Commit it
            logger.debug(f"get_config(name={name}, default={default}, create={create}) -> {default}")
            return default # Return default value
    logger.debug(f"get_config(name={name}, default={default}, create={create}) -> {data[1]}")
    return data[1] # Return config value

def set_config(name: str, value: str): # Set config value
    logger.debug(f"set_conifg(name={name}, value={value})")
    cursor.execute("UPDATE config SET value = ? WHERE name = ?", (value, name)) # Update config value

def next_id() -> int: # Get next id(for music registration)
    n = int(get_config("next_id", 0, True)) # Get current next_id
    set_config("next_id", str(n+1)) # Set next_id+1
    logger.debug(f"next_id() -> {n}")
    return n # Return next_id

def register_music(title: str, artist: str, file_mp3: str=None, file_flac: str=None, json_meta: str=""):
    if file_mp3 == None and file_flac == None:
        raise Exception(f"No music files provided.")
    if file_mp3 == None and file_flac != None:
        spl = path.splitext(file_flac)[0]
        popen(FFMPEG_CONV_TO_MP3.format(inp=file_flac, out=spl+".mp3"))
        file_mp3 = spl+".mp3"
    if file_flac == None and file_mp3 != None:
        spl = path.splitext(file_mp3)[0]
        popen(FFMPEG_CONV_TO_MP3.format(inp=file_mp3, out=spl+".flac"))
        file_flac = spl+".flac"
    nxt_id = next_id()
    cursor.execute(INSERT_MUSIC, (f"{artist} - {title}", title, artist, nxt_id))
    if isinstance(json_meta, (dict, list)):
        json_meta = dumps(json_meta, indent=4)
    cursor.execute(INSERT_MUSIC_INFO, (nxt_id, file_mp3, file_flac, json_meta))
    logger.debug(f"register_music(title={title}, artist={artist}, file_mp3={file_mp3}, file_flac={file_flac})")

def get_music_by_id(id: int) -> MusicInfo:
    cursor.execute(GET_BY_ID, (id,))
    data1 = cursor.fetchone()
    cursor.execute(GET_BY_ID2, (id,))
    data2 = cursor.fetchone()
    return MusicInfo(id, data1[0], data1[1], data1[2], data2[2], data2[3])

def search_by_name(name: str) -> list[int]:
    cursor.execute(SEARCH_BY_NAME, (name,))
    out = []
    for (i,_) in cursor.fetchall():
        out.append(i)
    return out

def _on_exit(): # Close database on exit
    logger.info("Closing database...")
    db.commit() # Commit all changes
    db.close() # Close database
    logger.success("Database closed")

register(_on_exit) # Register _on_exit function to close database on exit