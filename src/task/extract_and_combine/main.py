from fastapi import FastAPI
import subprocess as sp
import os
from os.path import join
import util
from google.cloud import storage
from google.cloud import bigquery

app = FastAPI()

os.environ['KAGGLE_USERNAME'] = "fahmiduldul"
os.environ['KAGGLE_KEY'] = "4a55c2dfac7f7191903a5abc5a4e8487"

@app.post("/extract")
def read_root():
    print(os.environ["KAGGLE_USERNAME"])
    sp.run(["mkdir", "payload"])
    sp.run(["kaggle", "datasets", "download", "edgartanaka1/tmdb-movies-and-series", "-p", "./payload"])
    sp.run(["unzip", "./payload/tmdb-movies-and-series.zip"])
    sp.run(["rm", "-rf", "./payload/tmdb-movies-and-series.zip"])
    sp.run(["mv", "-f", "./series/series", "./payload/series"])
    sp.run(["mv", "-f", "./movies/series", "./payload/movies"])

    for folder in ["series", "movies"]:
        dir = join("./payload", folder)
        out_file = join("./payload", folder+"_joined.json")
        util.combine_all_file(dir, out_file)

    client = storage.Client()
    bucket = client.bucket("de-porto")
    for folder in ["series", "movies"]:
        filename = folder+"_joined.json"
        dir = join("./payload", filename)
        blob = bucket.blob(join("qoala", filename))
        blob.upload_from_filename(dir)


    return {"status": "success"}
