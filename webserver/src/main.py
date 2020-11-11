import os
import logging
from service.insert import do_insert_audio
from service.search import do_search_audio
from service.count import do_count_table
from service.delete import do_delete_table
from indexer.index import milvus_client
from indexer.tools import connect_mysql
from common.config import UPLOAD_PATH
import time
from fastapi import FastAPI
from fastapi import File
from fastapi import UploadFile
import uvicorn
from starlette.responses import FileResponse
from starlette.requests import Request
import zipfile
from pathlib import Path
import uuid
from starlette.middleware.cors import CORSMiddleware


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"])


def init_conn():
    conn = connect_mysql()
    cursor = conn.cursor()
    index_client = milvus_client()
    return index_client, conn, cursor


def unzip_file(zip_src, dst_dir):
    r = zipfile.is_zipfile(zip_src)
    if r:
        with zipfile.ZipFile(zip_src, 'r') as f:
            for fn in f.namelist():
                extracted_path = Path(f.extract(fn, dst_dir))
                extracted_path.rename(dst_dir +'/' + fn.encode('cp437').decode('gbk'))
            return f.namelist()[0].encode('cp437').decode('gbk')
    else:
        return 'This is not zip'


@app.get('/countTable')
async def do_count_images_api(table_name: str = None):
    try:
        index_client, conn, cursor = init_conn()
        rows_milvus, rows_mysql = do_count_table(index_client, conn, cursor, table_name)
        return "{0},{1}".format(rows_milvus, rows_mysql), 200
    except Exception as e:
        logging.error(e)
        return "Error with {}".format(e), 400


@app.delete('/deleteTable')
async def do_delete_table_api(table_name: str = None):
    try:
        index_client, conn, cursor = init_conn()
        status = do_delete_table(index_client, conn, cursor, table_name)
        return "{}".format(status)
    except Exception as e:
        logging.error(e)
        return "Error with {}".format(e), 400


@app.get('/getAudio')
async def image_endpoint(audio: str):
    try:
        print("load img:", audio)
        return FileResponse(audio)
    except Exception as e:
        logging.error(e)
        return None, 200


@app.post('/insertAudio')
async def do_insert_audio_api(file: UploadFile=File(...), table_name: str = None):
    try:
        fname_path = UPLOAD_PATH + "/" + file.filename
        zip_file = await file.read()
        with open(fname_path,'wb') as f:
            f.write(zip_file)   
        audio_path = unzip_file(fname_path, UPLOAD_PATH)
        print("fname_path:", fname_path, "audio_path:", audio_path)
        os.remove(fname_path)

        index_client, conn, cursor = init_conn()
        info = do_insert_audio(index_client, conn, cursor, table_name, UPLOAD_PATH + "/" + audio_path)
        return info, 200
    except Exception as e:
        logging.error(e)
        return "Error with {}".format(e), 400


@app.post('/searchAudio')
async def do_search_audio_api(request: Request, audio: UploadFile = File(...), table_name: str = None):
    try:
        content = await audio.read()
        filename = UPLOAD_PATH + "/" + audio.filename
        with open(filename, "wb") as f:
            f.write(content)

        index_client, conn, cursor = init_conn()
        host = request.headers['host']
        milvus_ids, milvus_distance, audio_ids = do_search_audio(index_client, conn, cursor, table_name, filename)
        
        result_dic = {"code": 0, "msg": "success"}
        results = []
        for i in range(len(milvus_ids)):
            re = {
                "id": milvus_ids[i],
                "distance": milvus_distance[i],
                "audio": "http://" + str(host) + "/getAudio?audio=" + audio_ids[i]
            }
            results.append(re)
        result_dic["data"] = results
        return result_dic, 200
    except Exception as e:
        logging.error(e)
        return "Error with {}".format(e), 400



if __name__ == '__main__':
    uvicorn.run(app=app, host='192.168.1.85', port=8002)
