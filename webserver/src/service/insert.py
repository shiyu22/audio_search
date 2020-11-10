from indexer.logs import write_log
from common.config import DEFAULT_TABLE, VECTOR_DIMENSION, METRIC_TYPE
from indexer.index import create_table_milvus, insert_vectors, create_index
from indexer.tools import connect_mysql, create_table_mysql, load_data_to_mysql
from indexer.logs import write_log
from encoder.encode import get_audio_embedding
import os
import uuid


def init_table(index_client, conn, cursor, table_name):
    if table_name not in index_client.list_collections():
        print("create table.")
        create_table_mysql(conn, cursor, table_name)
        create_table_milvus(index_client, table_name, VECTOR_DIMENSION)
        create_index(index_client, table_name, METRIC_TYPE)


def get_ids_file(ids_milvus, ids_image, file_name):
    with open(file_name,'w') as f:
        for i in range(len(ids_image)):
            line = str(ids_milvus[i]) + "," + ids_image[i] + '\n'
            f.write(line)


def do_insert_audio(index_client, conn, cursor, table_name, audio_path):
    if not table_name:
        table_name = DEFAULT_TABLE
    
    try:
        init_table(index_client, conn, cursor, table_name)
        wavs = os.listdir(audio_path)
        wavs.sort()
        print("-----", len(wavs), wavs)
        embeddings = []
        for wav in wavs:
            if ".wav" in wav:
                embeddings.append(get_audio_embedding(audio_path + '/' + wav))
        ids_milvus = insert_vectors(index_client, table_name, embeddings)
        
        file_name = str(uuid.uuid1()) + ".csv"
        get_ids_file(ids_milvus, wavs, file_name)
        print("load data to mysql:", file_name)
        load_data_to_mysql(conn, cursor, table_name, file_name)

        return status, info
    except Exception as e:
        # log.error(e)
        write_log(e, 1)
        return None, "Error with {}".format(e)