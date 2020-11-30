from audio.indexer.logs import write_log
from audio.common.config import DEFAULT_TABLE, VECTOR_DIMENSION, METRIC_TYPE
from audio.indexer.index import create_table_milvus, insert_vectors, create_index
from audio.indexer.tools import connect_mysql, create_table_mysql, load_data_to_mysql
from audio.indexer.logs import write_log
from audio.encoder.encode import get_audio_embedding
import os
import uuid
import matplotlib.pyplot as plt
from scipy import signal
from scipy.io import wavfile


def init_table(index_client, conn, cursor, table_name):
    if table_name not in index_client.list_collections():
        print("create table.")
        create_table_mysql(conn, cursor, table_name)
        create_table_milvus(index_client, table_name, VECTOR_DIMENSION)
        create_index(index_client, table_name, METRIC_TYPE)


def get_ids_file(ids_milvus, ids_audio, file_name):
    with open(file_name,'w') as f:
        for i in range(len(ids_audio)):
            line = str(ids_milvus[i]) + "," + ids_audio[i] + '\n'
            f.write(line)


def get_spectorgram(audio_path, wav):
    sample_rate, samples = wavfile.read(audio_path + '/' + wav)
    frequencies, times, spectrogram = signal.spectrogram(samples, sample_rate)
    plt.pcolormesh(times, frequencies, spectrogram)
    plt.imshow(spectrogram)
    plt.ylabel('Frequency [Hz]')
    plt.xlabel('Time [sec]')
    plt.savefig(audio_path + '/' + wav.replace('.wav', '.jpg'))


def do_insert_audio(index_client, conn, cursor, table_name, audio_path):
    
    try:
        init_table(index_client, conn, cursor, table_name)
        wavs = os.listdir(audio_path)
        wavs.sort()
        embeddings = []
        ids_audio = []
        for wav in wavs:
            print("------------wav:", wav)
            if ".wav" in wav:
                ids_audio.append(audio_path + '/' + wav)
                get_spectorgram(audio_path, wav)
                embeddings.append(get_audio_embedding(audio_path + '/' + wav))
                print("len--------", len(embeddings))
        ids_milvus = insert_vectors(index_client, table_name, embeddings)
        
        file_name = str(uuid.uuid1()) + ".csv"
        get_ids_file(ids_milvus, ids_audio, file_name)
        print("load data to mysql:", file_name)
        load_data_to_mysql(conn, cursor, table_name, file_name)

        return "insert successfully!"
    except Exception as e:
        # log.error(e)
        write_log(e, 1)
        return "Error with {}".format(e)