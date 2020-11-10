import librosa
import panns_inference
from panns_inference import AudioTagging, SoundEventDetection, labels
from milvus import Milvus, DataType
import os
import numpy as np


def get_audio_embedding(path):
    audio, _ = librosa.core.load(path, sr=32000, mono=True)
    audio = audio[None, :]
    at = AudioTagging(checkpoint_path=None, device='cuda')
    _, embedding = at.inference(audio)
    embedding = embedding/np.linalg.norm(embedding)
    embedding = embedding.tolist()[0]
    return embeddings