from loguru import logger
__name__ = "Main app"
logger.info("Loading...")

import db
from fastapi import FastAPI
from fastapi.responses import FileResponse, RedirectResponse, Response
from pydantic import BaseModel

app = FastAPI(debug=True)

class GetByIDResponse(BaseModel):
    id: int
    name: str
    title: str
    artist: str

@app.get("/api/get_by_id")
def get_by_id(id: int) -> GetByIDResponse:
    d = db.get_music_by_id(id)
    return {'id': id, 'name': d.name, 'title': d.title, 'artist': d.artist}

@app.get("/api/get_mp3", description="Sends music in mp3 format")
def get_mp3(id: int):
    d = db.get_music_by_id(id)
    if d.file_mp3 == "NONE":
        return Response(status_code=204)
    if d.file_mp3.startswith("http://"):
        return RedirectResponse(d.file_mp3)
    return FileResponse(d.file_mp3, filename=f"{d.name}.mp3", media_type="audio/mpeg")

@app.get("/api/get_flac", description="Sends music in flac format")
def get_flac(id: int):
    d = db.get_music_by_id(id)
    if d.file_flac == "NONE":
        return Response(status_code=204)
    if d.file_flac.startswith("http://"):
        return RedirectResponse(d.file_mp3)
    return FileResponse(d.file_flac, filename=f"{d.name}.flac", media_type="audio/flac")

@app.get("/api/search_by_name", response_description="A list of ids that can be used to get music info", description="Searches in database by name(artist - title)")
def search_by_name(name: str) -> list[int]:
    return db.search_by_name(name)

if __name__ == '__main__':
    logger.error("This server can be only run using uvicorn/fastapi or whatever server software for python")