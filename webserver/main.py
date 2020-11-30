import os
import logging
from audio.service.insert import do_insert_audio as audio_insert_audio
from audio.service.search import do_search_audio as audio_search_audio
from audio.service.count import do_count_table as audio_count_table
from audio.service.delete import do_delete_table as audio_delete_table
from audio.indexer.index import milvus_client as audio_milvus_client
from audio.indexer.tools import connect_mysql as audio_connect_mysql
from audio.common.config import UPLOAD_PATH as audio_UPLOAD_PATH
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


def audio_init_conn():
    conn = audio_connect_mysql()
    cursor = conn.cursor()
    index_client = audio_milvus_client()
    return index_client, conn, cursor


def unzip_file(zip_src, dst_dir):
    r = zipfile.is_zipfile(zip_src)
    if r:
        with zipfile.ZipFile(zip_src, 'r') as f:
            # print("f.namelist()--------------",f.namelist())
            for fn in f.namelist():
                extracted_path = f.extract(fn, dst_dir)
                # extracted_path.rename(dst_dir +'/' + fn.encode('cp437').decode('gbk'))
            # return f.namelist()[0].encode('cp437').decode('gbk')
           	print("--------------",f.namelist()[0])
            return f.namelist()[0]
    else:
        print('This is not zip')
        return 'This is not zip'


@app.get('/countTable')
async def do_count_table_api(table_name: str = None):
    try:
        index_client, conn, cursor = audio_init_conn()
        rows_milvus, rows_mysql = audio_count_table(index_client, conn, cursor, table_name)
        return "{0},{1}".format(rows_milvus, rows_mysql), 200
    except Exception as e:
        logging.error(e)
        return "Error with {}".format(e), 400


@app.delete('/deleteTable')
async def do_delete_table_api(table_name: str = None):
    try:
        index_client, conn, cursor = audio_init_conn()
        status = audio_delete_table(index_client, conn, cursor, table_name)
        return "{}".format(status)
    except Exception as e:
        logging.error(e)
        return "Error with {}".format(e), 400


@app.get('/getAudio')
async def audio_endpoint(audio: str):
    try:
        print("load img:", audio)
        return FileResponse(audio)
    except Exception as e:
        logging.error(e)
        return None, 200


@app.post('/insertAudio')
async def do_insert_audio_api(file: UploadFile=File(...), table_name: str = None):
    try:
        fname_path = audio_UPLOAD_PATH + "/" + file.filename
        zip_file = await file.read()
        with open(fname_path,'wb') as f:
            f.write(zip_file)
        print("fname_path:", fname_path)

        audio_path = unzip_file(fname_path, audio_UPLOAD_PATH)
        print("fname_path:", fname_path, "audio_path:", audio_path)
        os.remove(fname_path)

        index_client, conn, cursor = audio_init_conn()
        info = audio_insert_audio(index_client, conn, cursor, table_name, audio_UPLOAD_PATH + "/" + audio_path)
        return info, 200
    except Exception as e:
        logging.error(e)
        return "Error with {}".format(e), 400


@app.post('/searchAudio')
async def do_search_audio_api(request: Request, audio: UploadFile = File(...), table_name: str = None):
    try:
        content = await audio.read()
        filename = audio_UPLOAD_PATH + "/" + audio.filename
        with open(filename, "wb") as f:
            f.write(content)

        index_client, conn, cursor = audio_init_conn()
        host = request.headers['host']
        milvus_ids, milvus_distance, audio_ids = audio_search_audio(index_client, conn, cursor, table_name, filename)
        
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
    uvicorn.run(app=app, host='0.0.0.0', port=8002)
